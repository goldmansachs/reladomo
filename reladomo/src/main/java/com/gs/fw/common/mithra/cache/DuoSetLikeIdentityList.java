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

import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.DoUntilProcedure2;
import com.gs.fw.common.mithra.util.DoUntilProcedure3;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;


public class DuoSetLikeIdentityList<T> extends AbstractSetLikeIdentityList<T>
{

    private T first;
    private T second;

    public DuoSetLikeIdentityList(T first, T second)
    {
        this.first = first;
        this.second = second;
    }

    public SetLikeIdentityList<T> addAndGrow(T toAdd)
    {
        if (toAdd == first || toAdd == second)
        {
            return this;
        }
        return new QuadSetLikeIdentityList(first, second, toAdd);
    }

    public boolean forAllWith(DoUntilProcedure2<T, Object> procedure, Object param)
    {
        if (!procedure.execute(first, param))
        {
            return procedure.execute(second, param);
        }
        return false;
    }

    public boolean forAllWith(DoUntilProcedure3<T, Object, Object> procedure, Object param1, Object param2)
    {
        if (!procedure.execute(first, param1, param2))
        {
            return procedure.execute(second, param1, param2);
        }
        return false;
    }

    public boolean forAll(DoUntilProcedure procedure)
    {
        if (!procedure.execute(first))
        {
            return procedure.execute(second);
        }
        return false;
    }

    public T getFirst()
    {
        return first;
    }

    public Object removeAndShrink(T toRemove)
    {
        if (toRemove == first)
        {
            return second;
        }
        else if (toRemove == second)
        {
            return first;
        }
        return this;
    }

    public boolean contains(Object o)
    {
        return o == first || o == second;
    }

    public boolean containsAll(Collection<?> c)
    {
        for (Iterator it = c.iterator(); it.hasNext(); )
        {
            Object o = it.next();
            if (o != first && o != second) return false;
        }
        return true;
    }

    public T get(int index)
    {
        if (index == 0) return first;
        if (index == 1) return second;
        throwArrayException(index);
        return null; // will never get here.
    }

    public int indexOf(Object o)
    {
        if (o == first) return 0;
        if (o == second) return 1;
        return -1;
    }

    public T set(int index, T element)
    {
        T replaced = null;
        if (index == 0)
        {
            replaced = first;
            first = element;
        }
        else if (index == 1)
        {
            replaced = second;
            second = element;
        }
        else
        {
            return throwArrayException(index);
        }
        return replaced;
    }

    private T throwArrayException(int index)
    {
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public int size()
    {
        return 2;
    }

    public List<T> subList(int fromIndex, int toIndex)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public Object[] toArray()
    {
        Object[] result = new Object[2];
        result[0] = first;
        result[1] = second;
        return result;
    }

    public <E> E[] toArray(E[] a)
    {
        if (a.length < 2)
            a = (E[]) java.lang.reflect.Array.
                    newInstance(a.getClass().getComponentType(), 2);
        a[0] = (E) first;
        a[1] = (E) second;
        if (a.length > 2)
        {
            a[2] = null;
        }
        return a;
    }
}
