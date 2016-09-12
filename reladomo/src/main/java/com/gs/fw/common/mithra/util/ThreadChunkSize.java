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


public class ThreadChunkSize
{
    private final int threads;
    private final int chunkSize;

    public ThreadChunkSize(int maxThreads, int size, int perItemWeight)
    {
        int threadsToUseInTotal = maxThreads;
        int nominalChunkSize = MithraCpuBoundThreadPool.WEIGHT_ONE_CHUNK_SIZE/perItemWeight;
        if (nominalChunkSize == 0) nominalChunkSize = 1;
        int compare = threadsToUseInTotal*nominalChunkSize;

        if (size < compare)
        {
            // multiplier is less than 1  - divide coll up into chunkSize based on : size of coll / chunk size
            threadsToUseInTotal = divideAndRoundUp(size, nominalChunkSize);
            nominalChunkSize = divideAndRoundUp(size, threadsToUseInTotal);
        }
        else
        {
            int multiplier = size/compare;
            if (multiplier > 16) multiplier = 16;
            nominalChunkSize = divideAndRoundUp(size, threadsToUseInTotal*multiplier);
        }
        this.threads = threadsToUseInTotal;
        this.chunkSize = nominalChunkSize;
    }

    public int divideAndRoundUp(int numerator, int denominator)
    {
        int result = numerator/denominator;
        if (numerator % denominator > 0)
        {
            result++;
        }
        return result;
    }

    public int getChunkSize()
    {
        return chunkSize;
    }

    public int getThreads()
    {
        return threads;
    }
}
