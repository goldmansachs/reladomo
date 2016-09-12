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

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.util.DoUntilProcedure;

import java.sql.Timestamp;
import com.gs.collections.impl.list.mutable.FastList;
import java.util.List;


public class MatchAllAsOfDatesProcedure implements DoUntilProcedure
{

    private AsOfExtractor[] extractors;
    private List result;
    private Timestamp[] asOfDates;
    private AsOfAttribute[] asOfAttributes;

    public MatchAllAsOfDatesProcedure(Extractor[] extractors, int extractorStartIndex, int initialSize, Timestamp[] asOfDates, AsOfAttribute[] asOfAttributes)
    {
        this.extractors = new AsOfExtractor[extractors.length - extractorStartIndex];
        for(int i=extractorStartIndex;i<extractors.length;i++)
        {
            this.extractors[i - extractorStartIndex] = (AsOfExtractor) extractors[i];
        }
        this.asOfDates = asOfDates;
        this.asOfAttributes = asOfAttributes;
        result = new FastList(initialSize);
    }

    public List getResult()
    {
        return result;
    }

    public boolean execute(Object o)
    {
        for(int i=0;i<extractors.length;i++)
        {
            if (!extractors[i].dataMatches(o, asOfDates[i], asOfAttributes[i]))
            {
                return false;
            }
        }
        result.add(o);
        return false;
    }
}
