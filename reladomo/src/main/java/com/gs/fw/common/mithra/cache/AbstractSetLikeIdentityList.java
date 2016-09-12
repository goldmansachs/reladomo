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

import java.util.List;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;


public abstract class AbstractSetLikeIdentityList<T> implements SetLikeIdentityList<T>, List<T>
{


    public List<T> getAll()
    {
        return this;
    }

    public void add(int index, T element)
    {
        throw new UnsupportedOperationException();
    }

    public boolean add(T o)
    {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(Collection<? extends T> c)
    {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(int index, Collection<? extends T> c)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty()
    {
        return false;
    }

    public Iterator<T> iterator()
    {
        return new SetLikeListIterator(this);
    }

    public int lastIndexOf(Object o)
    {
        return indexOf(o); // this is set like, so it can't be repeated
    }

    public ListIterator<T> listIterator()
    {
        return new SetLikeListIterator(this);
    }

    public ListIterator<T> listIterator(int index)
    {
        return new SetLikeListIterator(this, index);
    }

    public T remove(int index)
    {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o)
    {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    public void clear()
    {
        throw new UnsupportedOperationException();
    }
}
