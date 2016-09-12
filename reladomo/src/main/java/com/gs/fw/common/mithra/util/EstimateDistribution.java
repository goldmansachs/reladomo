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

package com.gs.fw.common.mithra.util;


public class EstimateDistribution
{
    private static final int LOG_SAMPLE_SIZE = 10;
    public static final int SAMPLE_SIZE = 1 << LOG_SAMPLE_SIZE;

    private static final double[] PRE_COMPUTED = preCompute();


    public static int estimateSize(int sizeAt1024, int sizeAt2048, int listSize)
    {
        if (sizeAt2048 == 2048)
        {
            return listSize;
        }
        double ratio = ((double)sizeAt1024)/sizeAt2048;
        if (ratio <= 0.5 || ratio >= 1.0)
        {
            return sizeAt2048;
        }
        double b = approximateValue(ratio);
        double a = findAsymptote(b, sizeAt2048);
        int scurve = (int) scurve(a, b, listSize);
        return scurve > listSize ? listSize : scurve;
    }

    private static double approximateValue(double ratio)
    {
        double arrayPos = (ratio - 0.5) * 200;
        int intPos = (int) arrayPos;
        if (arrayPos < intPos)
        {
            intPos--;
        }
        return PRE_COMPUTED[intPos] + (arrayPos - intPos) * (PRE_COMPUTED[(intPos + 1)] - PRE_COMPUTED[intPos]);
    }

    private static double scurve(double a, double b, int size)
    {
//        if (b * size > 10) return a;
        return fun(a, b, size);
    }

    private static double[] preCompute()
    {
        double[] precomputed = new double[100];
        double start = 0.5;
        double end = 1;
        double step = (end - start)/100;

        for(int i=0;i<100;i++)
        {
            double ratio = start + step * i;
            precomputed[i] = bisectValue(ratio);
        }
        return precomputed;
    }

    public static double bisectValue(double ratio)
    {
        double start = 1e-15;
        double end = 0.1;

        double guess = (end + start)/2;
        double diff = scurveRatio(guess) - ratio;
        while(Math.abs(diff) > 0.000001)
        {
            if (diff > 0)
            {
                end = guess;
            }
            else
            {
                start = guess;
            }
            guess = (end + start)/2;
            diff = scurveRatio(guess) - ratio;
        }
        return guess;
    }

    private static double scurveRatio(double guess)
    {
        return fun(guess, SAMPLE_SIZE)/fun(guess, SAMPLE_SIZE*2);
    }

    private static double findAsymptote(double b, double size)
    {
        return size/fun(b, SAMPLE_SIZE*2);
    }

    private static double fun(double a, double b, double x)
    {
        return a*fun(b, x);
    }

    private static double fun(double b, double x)
    {
        return fun(b * x);
    }

    private static double fun(double x)
    {
        return x/(x+1);
//        return ((x+1)*(x+1)-1)/((x+1)*(x+1) + 1);
//        return ((x+0.5)*(x+0.5)-0.5*0.5)/((x+0.5)*(x+0.5) + 1);
//        return ((x+0.1)*(x+0.1)-0.1*0.1)/((x+0.1)*(x+0.1) + 1);
//        return (Math.pow(2, x) -1)/(Math.pow(2, x) + 1);
    }

    public static int estimateMaxReturnSize(int multiplier, int maxSize, int totalSize, int avgSize)
    {
        int maxPowerOfTwo = Integer.highestOneBit(totalSize) + 1;
        int avgPowerOfTwo = Integer.highestOneBit(avgSize) + 1;
        int result = 0;
        int outliers = maxPowerOfTwo - avgPowerOfTwo;
        if (outliers > multiplier)
        {
            outliers = multiplier;
        }
        for(int i=0;i< outliers;i++)
        {
            result += maxSize;
            maxSize = maxSize >> 1;
        }
        result += avgSize * (multiplier - outliers);
        return result;
    }
}
