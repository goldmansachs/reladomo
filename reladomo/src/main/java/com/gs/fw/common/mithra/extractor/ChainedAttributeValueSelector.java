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

package com.gs.fw.common.mithra.extractor;

import com.gs.collections.api.block.function.Function;



public class ChainedAttributeValueSelector implements Function
{

    private Function first;
    private Function second;

    public ChainedAttributeValueSelector(Function first, Function second)
    {
        this.first = first;
        this.second = second;
    }

    public Object valueOf(Object anObject)
    {
        Object result = first.valueOf(anObject);
        if (result == null) return null;
        return second.valueOf(result);
    }
}
