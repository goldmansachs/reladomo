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

package com.gs.fw.common.mithra.tempobject;

import com.gs.collections.api.block.function.Function;

import java.util.AbstractList;
import java.util.List;


public class LazyListAdaptor<X, Y> extends AbstractList
{

    private List<X> delegateList;
    private Function<X, Y> function;

    public LazyListAdaptor(List<X> delegateList, Function<X, Y> function)
    {
        this.function = function;
        this.delegateList = delegateList;
    }

    public Y get(int index)
    {
        return function.valueOf(this.delegateList.get(index));
    }

    public int size()
    {
        return this.delegateList.size();
    }
}
