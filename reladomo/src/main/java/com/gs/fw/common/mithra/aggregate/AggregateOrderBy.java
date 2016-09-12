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

package com.gs.fw.common.mithra.aggregate;

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.finder.OrderBy;



public class AggregateOrderBy implements OrderBy
{

    private String attributeName;
    private boolean isAscending;

    public AggregateOrderBy(String name, boolean ascending)
    {
        this.attributeName = name;
        isAscending = ascending;
    }


    public int compare(Object left, Object right)
    {
        int result = 0;

        boolean leftNull = ((AggregateData) left).isAttributeNull(attributeName);
        boolean rightNull = ((AggregateData) right).isAttributeNull(attributeName);
        if (leftNull)
        {
            if (rightNull)
            {
                result = 0;
            }
            else
            {
                result = -1;
            }
        }
        else if (rightNull)
        {
            result = 1;
        }
        if (!(leftNull || rightNull))
        {
            Comparable comparableLeft = (Comparable) ((AggregateData) left).getValue(attributeName);
            Comparable comparableRight = (Comparable) ((AggregateData) right).getValue(attributeName);
            result = comparableLeft.compareTo(comparableRight);

        }
        if (!isAscending) result = -result;
        return result;
    }


    public com.gs.fw.finder.OrderBy and(com.gs.fw.finder.OrderBy other)
    {
        return new AggregateChainedOrderBy(this, other);
    }
}

