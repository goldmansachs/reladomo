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

package com.gs.fw.common.mithra.cache;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.util.MithraFastList;

import java.sql.Timestamp;
import java.util.List;


public class ParallelMatchAllAsOfDatesProcedure implements ParallelProcedure
{
    private AsOfExtractor[] extractors;
    private MithraFastList[] results;
    private Timestamp[] asOfDates;
    private AsOfAttribute[] asOfAttributes;

    public ParallelMatchAllAsOfDatesProcedure(Extractor[] extractors, int extractorStartIndex, Timestamp[] asOfDates, AsOfAttribute[] asOfAttributes)
    {
        this.extractors = new AsOfExtractor[extractors.length - extractorStartIndex];
        for(int i=extractorStartIndex;i<extractors.length;i++)
        {
            this.extractors[i - extractorStartIndex] = (AsOfExtractor) extractors[i];
        }
        this.asOfDates = asOfDates;
        this.asOfAttributes = asOfAttributes;
    }

    public MithraFastList[] getResult()
    {
        return results;
    }

    public void execute(Object o, int thread)
    {
        for(int i=0;i<extractors.length;i++)
        {
            if (!extractors[i].dataMatches(o, asOfDates[i], asOfAttributes[i]))
            {
                return;
            }
        }
        results[thread].add(o);
    }

    public void setThreads(int threads, int expectedCallsPerChunk)
    {
        results = new MithraFastList[threads];
        for(int i=0;i< threads;i++)
        {
            results[i] = new MithraFastList(expectedCallsPerChunk);
        }
    }
}