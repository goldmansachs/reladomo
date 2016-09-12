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
import com.gs.fw.common.mithra.util.DoUntilProcedure;

import java.sql.Timestamp;



public class MatchAsOfDatesProcedure implements DoUntilProcedure
{

    private AsOfAttribute[] asOfAttributes;
    private Timestamp[] asOfDates;
    private Object result;

    public MatchAsOfDatesProcedure(AsOfAttribute[] asOfAttributes)
    {
        this.asOfAttributes = asOfAttributes;
    }

    public void init(Timestamp[] asOfDates)
    {
        this.asOfDates = asOfDates;
        this.result = null;
    }

    public Object getResult()
    {
        return result;
    }

    public boolean execute(Object o)
    {
        for(int i=0;i<asOfAttributes.length;i++)
        {
            if (!asOfAttributes[i].dataMatches(o, asOfDates[i])) return false;
        }
        result = o;
        return true;
    }
}
