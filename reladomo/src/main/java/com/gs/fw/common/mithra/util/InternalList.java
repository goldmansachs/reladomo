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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Comparator;


public class InternalList implements Externalizable
{

    private static final int EMPTY_LIST_HASH = 728354663;

    private Object[] list;
    private int size;
    private static final Object[] EMPTY_ARRAY = new Object[0];

    public InternalList()
    {
        this.list = EMPTY_ARRAY;
    }

    public InternalList(int size)
    {
        this.list = size == 0 ? EMPTY_ARRAY : new Object[size];
    }

    public InternalList(Collection c)
    {
        this.list = new Object[c.size()];
        for(Object o: c)
        {
            this.list[size++] = o;
        }
    }

    public void add(Object o)
    {
        if (list.length <= size)
        {
            doubleCapacity();
        }
        list[size++] = o;
    }

    private void doubleCapacity()
    {
        Object[] tmp = this.list;
        int oldLength = tmp.length;
        if (oldLength == 0)
        {
            this.list = new Object[10];
        }
        else
        {
            this.list = new Object[oldLength << 1];
            System.arraycopy(tmp, 0, this.list, 0, oldLength);
        }
    }

    public Object get(int index)
    {
        return this.list[index];
    }

    public int size()
    {
        return this.size;
    }

    public void sort(Comparator comparator)
    {
        if (size > 1)
        {
            CollectionUtil.psort(this.list, size, comparator);
        }
    }

    public void sort()
    {
        if (size > 1)
        {
            CollectionUtil.psort(this.list, size, CollectionUtil.COMPARABLE_COMPARATOR);
        }
    }

    public void clear()
    {
        int size = this.size;
        if (size > 0)
        {
            for(int i=0;i< size;i++)
            {
                this.list[i] = null;
            }
            this.size = 0;
        }
    }

    public void ensureCapacity(int newCapacity)
    {
        if (this.list.length < newCapacity)
        {
            Object[] tmp = this.list;
            this.list = new Object[newCapacity];
            System.arraycopy(tmp, 0, this.list, 0, tmp.length);
        }
    }

    public void addAll(InternalList other)
    {
        ensureCapacity(this.size+other.size);
        int start = this.size;
        this.size += other.size;
        for(int i=0;i<other.size;i++)
        {
            this.list[start++] = other.list[i];
        }
    }

    public void removeByReplacingFromEnd(int index)
    {
        int size = --this.size;
        if (index != size)
        {
            list[index] = list[size];
        }
        list[size] = null;
    }

    public void set(int index, Object replacement)
    {
        this.list[index] = replacement;
    }

    public void add(int index, Object o)
    {
        ensureCapacity(size+1);
        if (index != size)
        {
            System.arraycopy(list, index, list, index + 1,size - index);
        }
        size++;
        list[index] = o;
    }


    public void addAll(int index, InternalList other)
    {
        int numNew = other.size;
        ensureCapacity(size + numNew);

        int numMoved = size - index;
        if (numMoved > 0)
        {
            System.arraycopy(this.list, index, this.list, index + numNew, numMoved);
        }
        System.arraycopy(other.list, 0, this.list, index, numNew);
        size += numNew;
    }

    public Object remove(int index)
    {
        Object oldValue = list[index];
        int size = --this.size;
        int numMoved = size - index;
        if (numMoved > 0)
        {
            System.arraycopy(list, index+1, list, index, numMoved);
        }
        list[size] = null;
        return oldValue;
    }

    public boolean contains(Object o)
    {
        int size = this.size;
        for(int i=0;i<size;i++)
        {
            if (this.list[i].equals(o)) return true;
        }
        return false;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(this.size);
        for(int i=0;i<this.size;i++)
        {
            out.writeObject(this.list[i]);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.size = in.readInt();
        this.list = new Object[this.size];
        for(int i=0;i<this.size;i++)
        {
            list[i] = in.readObject();
        }
    }

    public boolean equals(Object obj)
    {
        return obj instanceof InternalList && equals((InternalList) obj);
    }

    public boolean equals(InternalList other)
    {
        if (other == this) return true;
        if (this.size != other.size) return false;
        int size = this.size;
        for(int i=0;i<size;i++)
        {
            Object o = list[i];
            if (!o.equals(other.list[i]))
            {
                return false;
            }
        }
        return true;
    }

    public int hashCode()
    {
        int result = EMPTY_LIST_HASH;
        int size = this.size;
        for(int i=0;i<size;i++)
        {
            result = HashUtil.combineHashes(result, this.list[i].hashCode());
        }
        return result;
    }

    public int orderIndependentHashCode()
    {
        int result = EMPTY_LIST_HASH;
        int size = this.size;
        for(int i=0;i<size;i++)
        {
            result ^= this.list[i].hashCode();
        }
        return result;
    }

    public boolean isEmpty()
    {
        return this.size == 0;
    }

    public InternalList copy()
    {
        int size = this.size;
        InternalList copy = new InternalList(size);
        if (size > 0)
        {
            System.arraycopy(list, 0, copy.list, 0, size);
            copy.size = size;
        }
        return copy;
    }

    public void toArray(Object[] objects)
    {
        System.arraycopy(this.list, 0, objects, 0, this.size);
    }

    public boolean remove(Object object)
    {
        int size = this.size;
        for(int i=0;i<size;i++)
        {
            if (list[i].equals(object))
            {
                remove(i);
                return true;
            }
        }
        return false;
    }

    public void swap(int first, int second)
    {
        Object tmp = list[first];
        list[first] = list[second];
        list[second] = tmp;
    }
}
