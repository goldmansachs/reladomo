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


public class QuadSetLikeIdentityList<T> extends AbstractSetLikeIdentityList<T>
{

    private T first;
    private T second;
    private T third;
    private T forth;

    public QuadSetLikeIdentityList(T first, T second, T third)
    {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public QuadSetLikeIdentityList(T first, T second, T third, T forth)
    {
        this.first = first;
        this.second = second;
        this.third = third;
        this.forth = forth;
    }

    public SetLikeIdentityList<T> addAndGrow(T toAdd)
    {
        if (toAdd == first || toAdd == second || toAdd == third)
        {
            return this;
        }
        if (forth == null)
        {
            forth = toAdd;
            return this;
        }
        if (forth == toAdd)
        {
            return this;
        }
        return new ArraySetLikeIdentityList(first, second, third, forth, toAdd);
    }

    public T getFirst()
    {
        return first;
    }

    public Object removeAndShrink(T toRemove)
    {
        boolean possibleShrink = false;
        if (forth == null) possibleShrink = true;
        if (toRemove == first)
        {
            if (possibleShrink)
            {
                return new DuoSetLikeIdentityList(second, third);
            }
            first = second;
            second = third;
            third = forth;
            forth = null;
        }
        else if (toRemove == second)
        {
            if (possibleShrink)
            {
                return new DuoSetLikeIdentityList(first, third);
            }
            second = third;
            third = forth;
            forth = null;
        }
        else if (toRemove == third)
        {
            if (possibleShrink)
            {
                return new DuoSetLikeIdentityList(first, second);
            }
            third = forth;
            forth = null;
        }
        else if (toRemove == forth)
        {
            forth = null;
        }
        return this;
    }

    public boolean contains(Object o)
    {
        return o == first || o == second || o == third || o == forth;
    }

    public boolean containsAll(Collection<?> c)
    {
        for (Iterator it = c.iterator(); it.hasNext(); )
        {
            Object o = it.next();
            if (o != first && o != second && o != third && o != forth) return false;
        }
        return true;
    }

    public T get(int index)
    {
        if (index == 0) return first;
        if (index == 1) return second;
        if (index == 2) return third;
        if (index == 3 && forth != null) return forth;
        throwArrayException(index);
        return null; // will never get here.
    }

    public int indexOf(Object o)
    {
        if (o == first) return 0;
        if (o == second) return 1;
        if (o == third) return 2;
        if (o == forth) return 3;
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
        else if (index == 2)
        {
            replaced = third;
            third = element;
        }
        else if (index == 3 && forth != null)
        {
            replaced = forth;
            forth = element;
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
        return forth == null ? 3 : 4;
    }

    public boolean forAll(DoUntilProcedure procedure)
    {
        boolean done = procedure.execute(first);
        if (!done)
        {
            done = procedure.execute(second);
        }
        if (!done)
        {
            done = procedure.execute(third);
        }
        if (!done && forth != null)
        {
            done = procedure.execute(forth);
        }
        return done;
    }

    public boolean forAllWith(DoUntilProcedure2 procedure, Object param)
    {
        boolean done = procedure.execute(first, param);
        if (!done)
        {
            done = procedure.execute(second, param);
        }
        if (!done)
        {
            done = procedure.execute(third, param);
        }
        if (!done && forth != null)
        {
            done = procedure.execute(forth, param);
        }
        return done;
    }

    public boolean forAllWith(DoUntilProcedure3 procedure, Object param1, Object param2)
    {
        boolean done = procedure.execute(first, param1, param2);
        if (!done)
        {
            done = procedure.execute(second, param1, param2);
        }
        if (!done)
        {
            done = procedure.execute(third, param1, param2);
        }
        if (!done && forth != null)
        {
            done = procedure.execute(forth, param1, param2);
        }
        return done;
    }

    public List<T> subList(int fromIndex, int toIndex)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public Object[] toArray()
    {
        Object[] result = new Object[size()];
        result[0] = first;
        result[1] = second;
        result[2] = third;
        if (forth != null) result[3] = forth;
        return result;
    }

    public <E> E[] toArray(E[] a)
    {
        int size = size();
        if (a.length < size)
            a = (E[]) java.lang.reflect.Array.
                    newInstance(a.getClass().getComponentType(), size);
        a[0] = (E) first;
        a[1] = (E) second;
        a[2] = (E) third;
        if (size == 4) a[3] = (E) forth;
        if (a.length > size)
        {
            a[size] = null;
        }
        return a;
    }
}
