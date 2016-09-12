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

import com.gs.collections.impl.list.mutable.FastList;

import java.util.Collection;


public class MithraFastList<T> extends FastList<T>
{
    private static final long serialVersionUID = 8782786230027662L;

    public MithraFastList()
    {
        // for Externalizable
        super();
    }

    public MithraFastList(int initialCapacity)
    {
        super(initialCapacity);
    }

    public MithraFastList(T[] array)
    {
        super(array);
    }

    public MithraFastList(int size, T[] array)
    {
        super(size, array);
    }

    public MithraFastList(Collection<? extends T> source)
    {
        super(source);
    }

    public static <E> MithraFastList<E> newList()
    {
        return new MithraFastList<E>();
    }

    public static <E> MithraFastList<E> newListWith(E... elements)
    {
        return new MithraFastList<E>(elements);
    }

    public void addWithSizePrediction(T toAdd, int countSoFar, int totalCount)
    {
        final int oldCapacity = this.items.length;
        if (this.size == oldCapacity)
        {
            int newCapacity = (int) ((double) totalCount * (double) oldCapacity / (double) countSoFar + 5);
            resizeToCapacity(newCapacity);
        }
        this.add(toAdd);
    }

    public void zEnsureCapacity(final int minCapacity)
    {
        final int oldCapacity = this.items.length;
        if (minCapacity > oldCapacity)
        {
            final int newCapacity = Math.max((oldCapacity * 3) / 2 + 1, minCapacity);
            resizeToCapacity(newCapacity);
        }
    }

    private void resizeToCapacity(int newCapacity)
    {
        if (newCapacity > this.items.length)
        {
            final Object[] newItems = new Object[newCapacity];
            System.arraycopy(this.items, 0, newItems, 0, Math.min(this.size, newCapacity));
            this.items = (T[]) newItems;
        }
    }

    public int moveBottom(MithraFastList<T> bigList, int toMove, int targetSize)
    {
        int toMoveWithinTarget = Math.min(targetSize - this.size(),  toMove);
        if (toMoveWithinTarget > 0)
        {
            resizeToCapacity(this.size() + toMoveWithinTarget);
            System.arraycopy(bigList.items, bigList.size() - toMoveWithinTarget, this.items, this.size(), toMoveWithinTarget);
            bigList.size -= toMoveWithinTarget;
            this.size += toMoveWithinTarget;
            return toMoveWithinTarget;
        }
        else return 0;
    }

    public void removeByReplacingFromEnd(int index)
    {
        if (index != size - 1)
        {
            this.set(index, this.get(this.size - 1));
        }
        this.remove(size - 1);
    }

    public void removeNullItems()
    {
        int currentFilledIndex = 0;
        for (int i = 0; i < this.size; i++)
        {
            T item = this.items[i];
            if (item != null)
            {
                // keep it
                if (currentFilledIndex != i)
                {
                    this.items[currentFilledIndex] = item;
                }
                currentFilledIndex++;
            }
        }
        for (int i = currentFilledIndex; i < this.size; i++)
        {
            this.items[i] = null;
        }
        this.size = currentFilledIndex;
    }
}
