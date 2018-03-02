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

package com.gs.fw.common.mithra.util;


import org.eclipse.collections.api.list.FixedSizeList;
import org.eclipse.collections.impl.factory.Lists;

import java.util.List;

public class ListFactory
{
    public static final List EMPTY_LIST = Lists.fixedSize.of();

    public static <T> FixedSizeList<T> create(T one)
    {
        return Lists.fixedSize.of(one);
    }

    public static <T> FixedSizeList<T> create(T one, T two)
    {
        return Lists.fixedSize.of(one, two);
    }

    public static <T> FixedSizeList<T> create(T one, T two, T three)
    {
        return Lists.fixedSize.of(one, two, three);
    }

    public static <T> FixedSizeList<T> create(T one, T two, T three, T four)
    {
        return Lists.fixedSize.of(one, two, three, four);
    }
}
