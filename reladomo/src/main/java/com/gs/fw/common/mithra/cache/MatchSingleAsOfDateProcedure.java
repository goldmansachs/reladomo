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

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.sql.Timestamp;
import java.util.List;


public class MatchSingleAsOfDateProcedure implements DoUntilProcedure
{

    private List result;
    private Timestamp asOfDate;
    private AsOfAttribute asOfAttribute;

    public MatchSingleAsOfDateProcedure(int initialSize, Timestamp asOfDate, AsOfAttribute asOfAttribute)
    {
        this.asOfDate = asOfDate;
        this.asOfAttribute = asOfAttribute;
        result = new FastList(initialSize);
    }

    public List getResult()
    {
        return result;
    }

    public boolean execute(Object o)
    {
        if (asOfAttribute.dataMatches(o, asOfDate))
        {
            result.add(o);
        }
        return false;
    }
}
