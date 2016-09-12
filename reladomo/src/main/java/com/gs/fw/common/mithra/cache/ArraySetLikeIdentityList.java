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


public class ArraySetLikeIdentityList<T> extends AbstractSetLikeIdentityList<T>
{
    private static final int MAX_TO_HOLD = 10;

    private T[] table;
    private int size;

    public ArraySetLikeIdentityList(List<T> list)
    {
        this.size = list.size();
        this.table = (T[]) new Object[size()];
        for (int i = 0; i < list.size(); i++) this.table[i] = list.get(i);
    }

    public ArraySetLikeIdentityList(T first, T second, T third, T forth, T fifth)
    {
        size = 5;
        table = (T[]) new Object[6];
        table[0] = first;
        table[1] = second;
        table[2] = third;
        table[3] = forth;
        table[4] = fifth;
    }

    public boolean forAllWith(DoUntilProcedure2<T, Object> procedure, Object param)
    {
        boolean done = false;
        for (int i = 0; i < this.size() && !done; i++)
        {
            done = procedure.execute(table[i], param);
        }
        return done;
    }

    public boolean forAllWith(DoUntilProcedure3<T, Object, Object> procedure, Object param1, Object param2)
    {
        boolean done = false;
        for (int i = 0; i < this.size() && !done; i++)
        {
            done = procedure.execute(table[i], param1, param2);
        }
        return done;
    }

    public boolean forAll(DoUntilProcedure procedure)
    {
        boolean done = false;
        for (int i = 0; i < this.size() && !done; i++)
        {
            done = procedure.execute(table[i]);
        }
        return done;
    }

    public SetLikeIdentityList<T> addAndGrow(T toAdd)
    {
        for (int i = 0; i < size; i++)
        {
            if (toAdd == table[i]) return this;
        }
        if (table.length < size)
        {
            table[size] = toAdd;
            size++;
            return this;
        }
        if (size + 1 < MAX_TO_HOLD)
        {
            T[] newTable = (T[]) new Object[size + 2];
            System.arraycopy(table, 0, newTable, 0, size);
            newTable[size] = toAdd;
            size++;
            table = newTable;
            return this;
        }
        FullUniqueIndex<T> index = new FullUniqueIndex<T>(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY, this.size + 2);
        for (int i = 0; i < size; i++)
        {
            index.put(table[i]);
        }
        index.put(toAdd);
        return index;
    }

    public T getFirst()
    {
        return table[0];
    }

    public Object removeAndShrink(T toRemove)
    {
        for (int i = 0; i < size; i++)
        {
            if (toRemove == table[i])
            {
                size--;
                table[i] = table[size];
                table[size] = null;
                if (size > 4)
                {
                    return this;
                }
                else
                {
                    return new QuadSetLikeIdentityList(table[0], table[1], table[2], table[3]);
                }
            }
        }
        return this;
    }

    public boolean contains(Object o)
    {
        for (int i = 0; i < size; i++)
        {
            if (o == table[i])
            {
                return true;
            }
        }
        return false;
    }

    public boolean containsAll(Collection<?> c)
    {
        for (Iterator it = c.iterator(); it.hasNext(); )
        {
            Object o = it.next();
            if (!contains(o)) return false;
        }
        return true;
    }

    public T get(int index)
    {
        if (index >= size) throwArrayException(index);
        return table[index];
    }

    public int indexOf(Object o)
    {
        for (int i = 0; i < size; i++)
        {
            if (o == table[i])
            {
                return i;
            }
        }
        return -1;
    }

    public T set(int index, T element)
    {
        if (index >= size)
        {
            return throwArrayException(index);
        }
        T replaced = table[index];
        table[index] = element;
        return replaced;
    }

    private T throwArrayException(int index)
    {
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public int size()
    {
        return size;
    }

    public List<T> subList(int fromIndex, int toIndex)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public Object[] toArray()
    {
        Object[] result = new Object[size];
        System.arraycopy(table, 0, result, 0, size);
        return result;
    }

    public <E> E[] toArray(E[] a)
    {
        if (a.length < size)
        {
            a = (E[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        }
        System.arraycopy(table, 0, a, 0, size);
        if (a.length > size)
        {
            a[size] = null;
        }
        return a;
    }
}
