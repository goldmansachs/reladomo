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

package com.gs.fw.common.mithra.finder.orderby;

import com.gs.fw.common.mithra.attribute.LongAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;



public class LongOrderBy extends AttributeBasedOrderBy
{

    public LongOrderBy(Attribute attribute, boolean ascending)
    {
        super(attribute, ascending);
    }

    protected int compareAscending(Object left, Object right)
    {
        long leftInt = ((LongAttribute)this.getAttribute()).longValueOf(left);
        long rightInt = ((LongAttribute)this.getAttribute()).longValueOf(right);
        return (leftInt < rightInt) ? -1 : ((leftInt == rightInt) ? 0 : 1);
    }
}
