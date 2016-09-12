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

import com.gs.fw.common.mithra.attribute.Attribute;


public class ByteArrayOrderBy extends AttributeBasedOrderBy
{

    public ByteArrayOrderBy(Attribute attribute, boolean ascending)
    {
        super(attribute, ascending);
    }

    protected int compareAscending(Object left, Object right)
    {
        byte[] leftValue = (byte[]) this.getAttribute().valueOf(left);
        byte[] rightValue = (byte[]) this.getAttribute().valueOf(right);
        if (leftValue.length > rightValue.length)
        {
            return compareWith(leftValue, rightValue);
        }
        else
        {
            return compareWith(rightValue, leftValue);
        }
    }

    private int compareWith(byte[] rightValue, byte[] leftValue)
    {
        for(int i=0;i<rightValue.length;i++)
        {
            int result = rightValue[i] - leftValue[i];
            if (result != 0) return result;
        }
        if (leftValue.length > rightValue.length) return 1;
        return 0;
    }
}
