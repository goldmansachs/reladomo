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

import java.util.ListIterator;
import java.util.List;


public class SetLikeListIterator<T> implements ListIterator<T>
{
    private int position = 0;
    private List<T> delegate;

    public SetLikeListIterator(List<T> delegate)
    {
        this.delegate = delegate;
    }

    public SetLikeListIterator(List<T> delegate, int position)
    {
        this.delegate = delegate;
        this.position = position;
    }

    public boolean hasNext()
    {
        return position < delegate.size();
    }

    public T next()
    {
        return delegate.get(position++);
    }

    public void add(T o)
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasPrevious()
    {
        return position > 0;
    }

    public int nextIndex()
    {
        return position;
    }

    public T previous()
    {
        return delegate.get(--position);
    }

    public int previousIndex()
    {
        return position - 1;
    }

    public void set(T o)
    {
        delegate.set(position - 1, o);
    }

    public void remove()
    {
        delegate.remove(position - 1);
    }
}
