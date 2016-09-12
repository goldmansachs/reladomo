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
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.util.DoUntilProcedure;

import java.util.List;


public class MatchAsOfExtractorProcedure implements DoUntilProcedure
{

    private int startIndex;
    private AsOfExtractor extractorOne;
    private AsOfExtractor extractorTwo;
    private boolean matchMoreThanOne = false;
    private Object result;
    private Object valueHolder;
    private AsOfAttribute[] asOfAttributes;

    public MatchAsOfExtractorProcedure(int startIndex, Object valueHolder, List extractors, AsOfAttribute[] asOfAttributes)
    {
        this.startIndex = startIndex;
        this.valueHolder = valueHolder;
        this.asOfAttributes = asOfAttributes;
        this.extractorOne = (AsOfExtractor) extractors.get(startIndex);
        this.matchMoreThanOne = extractorOne.matchesMoreThanOne();
        if (extractors.size() - startIndex > 1)
        {
            this.extractorTwo = (AsOfExtractor) extractors.get(startIndex + 1);
            this.matchMoreThanOne = this.matchMoreThanOne || this.extractorTwo.matchesMoreThanOne();
        }
    }

    public MatchAsOfExtractorProcedure(int startIndex, Object valueHolder, Extractor[] extractors, AsOfAttribute[] asOfAttributes)
    {
        this.startIndex = startIndex;
        this.valueHolder = valueHolder;
        this.asOfAttributes = asOfAttributes;
        this.extractorOne = (AsOfExtractor) extractors[startIndex];
        this.matchMoreThanOne = extractorOne.matchesMoreThanOne();
        if (extractors.length - startIndex > 1)
        {
            this.extractorTwo = (AsOfExtractor) extractors[startIndex + 1];
            this.matchMoreThanOne = this.matchMoreThanOne || this.extractorTwo.matchesMoreThanOne();
        }
    }

    public Object getResult()
    {
        return result;
    }

    public boolean execute(Object o)
    {
        if (!extractorOne.dataMatches(o, extractorOne.timestampValueOf(valueHolder), asOfAttributes[0]))
        {
            return false;
        }
        if (extractorTwo != null && !extractorTwo.dataMatches(o, extractorTwo.timestampValueOf(valueHolder), asOfAttributes[1]))
        {
            return false;
        }
        if (this.matchMoreThanOne)
        {
            if (result == null)
            {
                result = o;
            }
            else
            {
                List list;
                if (result instanceof FastList)
                {
                    list = (List) result;
                }
                else
                {
                    list = new FastList(3);
                    list.add(result);
                    result = list;
                }
                list.add(o);
            }
            return false;
        }
        else
        {
            result = o;
            return true;
        }
    }
}
