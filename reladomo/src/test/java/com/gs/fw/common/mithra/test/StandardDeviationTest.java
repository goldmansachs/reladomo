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

package com.gs.fw.common.mithra.test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import com.gs.collections.api.block.predicate.Predicate;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.utility.internal.IteratorIterate;
import com.gs.fw.common.mithra.util.MutableStandardDeviation;

public class StandardDeviationTest extends MithraTestAbstract
{
    private double m2comp = 0.0;
    private double meanComp = 0.0;

    public void testStdDev1()
    {
        Random random = new Random(0xABCDEFL);
        int arraySize = 1000000;
        double[] array = new double[arraySize];
        MutableStandardDeviation standardDeviation = new MutableStandardDeviation();
//        MutableStandardDeviation_sos standardDeviation_sos = new MutableStandardDeviation_sos();
        for(int i = 0; i < arraySize; i++)
        {
            double v = Math.floor(random.nextDouble() * 100000) / 100000;
//            double value = v + 1000000000;
            array[i] = v;
        }

//        Arrays.sort(array);
        double sum = 0;
        for(int i = 0; i < arraySize; i++)
        {
            sum = adjustSum(sum, array[i]);
        }

        double mean = sum / arraySize;

        double varianceSum = 0;
        for(int i = 0; i < arraySize; i++)
        {
            varianceSum = adjustSumMean(varianceSum, (array[i] - mean) * (array[i] - mean));
        }
        double standardDeviationByHand = Math.sqrt(varianceSum / (arraySize - 1));

        for(int i = 0; i < arraySize; i++)
        {
            standardDeviation.add(array[i] + 1000000000);
//            standardDeviation_sos.add(array[i]);
        }

        double welford = standardDeviation.doubleValue();
//        double sumOfSquares = standardDeviation_sos.doubleValue();

        assertEquals(standardDeviationByHand, welford);
//        assertEquals(standardDeviationByHand, sumOfSquares);
//
//        BigDecimal sum2 = new BigDecimal(0);
//        for(int i = 0; i < arraySize; i++)
//        {
//            sum2 = sum2.add(new BigDecimal(array[i]));
//        }
//
//        BigDecimal mean2 = sum2.divide(new BigDecimal(arraySize));
//
//        BigDecimal varianceSum2 = new BigDecimal(0);
//        for(int i = 0; i < arraySize; i++)
//        {
//            BigDecimal one = new BigDecimal(array[i]).subtract(mean2);
//            varianceSum2 = varianceSum2.add(one.multiply(one));
//        }
//        double standardDeviationByHand2 = Math.sqrt(varianceSum2.divide(new BigDecimal(arraySize - 1), 17, BigDecimal.ROUND_HALF_UP).doubleValue());
//
//        for(int i = 0; i < arraySize; i++)
//        {
//            standardDeviation.add(array[i] + 1000000000);
//            standardDeviation_sos.add(array[i] + 1000000000);
//        }
//
//        assertEquals(standardDeviationByHand2, welford);
//        assertEquals(standardDeviationByHand2, sumOfSquares);
    }

    private double adjustSum(double sumDouble, double newValue)
    {
        double adjustedValue = newValue - this.m2comp;
        double nextSum = sumDouble + adjustedValue;
        this.m2comp = (nextSum - sumDouble) - adjustedValue;
        sumDouble = nextSum;
        return sumDouble;
    }

    private double adjustSumMean(double sumDouble, double newValue)
    {
        double adjustedValue = newValue - this.meanComp;
        double nextSum = sumDouble + adjustedValue;
        this.meanComp = (nextSum - sumDouble) - adjustedValue;
        sumDouble = nextSum;
        return sumDouble;
    }

    public void shuffle(double[] array, Random rnd)
    {
        for (int i = array.length; i > 1; i--)
        {
            swap(array, i - 1, rnd.nextInt(i));
        }
    }

    private static void swap(double[] arr, int i, int j)
    {
        double tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}
