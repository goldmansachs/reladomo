

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

package com.gs.fw.common.mithra.util;

import com.gs.fw.common.mithra.finder.Operation;

import java.sql.Timestamp;
import java.util.Map;

public class Pair<T1, T2>
{
    private static final long serialVersionUID = 1L;

    private final T1 one;
    private final T2 two;

    public static <T1, T2> Pair<T1, T2> of(T1 one, T2 two)
    {
        return new Pair<T1, T2>(one, two);
    }

    public Pair(T1 newOne, T2 newTwo)
    {
        this.one = newOne;
        this.two = newTwo;
    }

    public T1 getOne()
    {
        return this.one;
    }

    public T2 getTwo()
    {
        return this.two;
    }

    private static boolean nullSafeEquals(Object value1, Object value2)
    {
        return value1 == null ? value2 == null : value1.equals(value2);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof Pair))
        {
            return false;
        }

        Pair<?, ?> that = (Pair<?, ?>) o;

        return nullSafeEquals(this.one, that.getOne())
                && nullSafeEquals(this.two, that.getTwo());
    }

    @Override
    public int hashCode()
    {
        return HashUtil.combineHashes(HashUtil.hash(this.one), HashUtil.hash(this.two));
    }

    @Override
    public String toString()
    {
        return this.one + ":" + this.two;
    }

}
