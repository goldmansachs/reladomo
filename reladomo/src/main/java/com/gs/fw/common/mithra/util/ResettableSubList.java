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


import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ResettableSubList implements List
{
    private List target;
    private int offset;
    private int size;

    public ResettableSubList()
    {
    }

    protected void reset(List target, int offset, int size)
    {
        this.target = target;
        this.offset = offset;
        this.size = size;
    }

    public Object get(int index)
    {
        return target.get(index + offset);
    }

    public Object set(int index, Object element)
    {
        return target.set(index + offset, element);
    }

    public Object remove(int index)
    {
        return target.remove(offset + index);
    }

    public int size()
    {
        return size;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public boolean add(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public void add(int index, Object element)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean addAll(Collection c)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean addAll(int index, Collection c)
    {
        throw new RuntimeException("not implemented");
    }

    public void clear()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean contains(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean containsAll(Collection c)
    {
        throw new RuntimeException("not implemented");
    }

    public int indexOf(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public Iterator iterator()
    {
        throw new RuntimeException("not implemented");
    }

    public int lastIndexOf(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public ListIterator listIterator()
    {
        throw new RuntimeException("not implemented");
    }

    public ListIterator listIterator(int index)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean remove(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean removeAll(Collection c)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean retainAll(Collection c)
    {
        throw new RuntimeException("not implemented");
    }

    public List subList(int fromIndex, int toIndex)
    {
        throw new RuntimeException("not implemented");
    }

    public Object[] toArray()
    {
        throw new RuntimeException("not implemented");
    }

    public Object[] toArray(Object[] a)
    {
        throw new RuntimeException("not implemented");
    }
}
