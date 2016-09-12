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

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;


public class MithraCompositeList<E>
        implements List<E>, Serializable
{
    private static final long serialVersionUID = 2802551683462247261L;
    private volatile FastList<MithraFastList<E>> lists;

    private static final Comparator<MithraFastList> REVERSE_SIZE_COMPARATOR = new Comparator<MithraFastList>()
    {
        public int compare(MithraFastList o1, MithraFastList o2)
        {
            return o2.size() - o1.size();
        }
    };

    public MithraCompositeList(int size)
    {
        lists = new FastList<MithraFastList<E>>(size);
    }

    public MithraCompositeList(MithraFastList[] results)
    {
        lists = new FastList<MithraFastList<E>>(results.length);
        for (MithraFastList list : results)
        {
            lists.add(list);
        }
        rebalance();
    }

    public FastList<MithraFastList<E>> getLists()
    {
        return lists;
    }

    private void rebalance()
    {
        if (this.size() > 1000000)
        {
            lists.sortThis(REVERSE_SIZE_COMPARATOR);
            int bigIndex = 0;
            int smallIndex = lists.size() - 1;
            int idealSize = this.size() / lists.size();
            int maxIdealSize = (int) (idealSize * 1.1);
            while (bigIndex < smallIndex)
            {
                MithraFastList<E> bigList = lists.get(bigIndex);
                if (bigList.size() > maxIdealSize)
                {
                    int toMove = bigList.size() - idealSize;
                    int moved = lists.get(smallIndex).moveBottom(bigList, toMove, idealSize);
                    if (moved < toMove)
                    {
                        smallIndex--;
                    }
                }
                else
                {
                    bigIndex++;
                }
            }
        }
    }

    public synchronized void synchronizedAddCompositedList(MithraFastList list)
    {
        this.lists.add(list);
    }

    public int size()
    {
        int size = 0;
        FastList<MithraFastList<E>> localLists = this.lists;
        for (int i = localLists.size() - 1; i >= 0; i--)
        {
            size += localLists.get(i).size();
        }
        return size;
    }

    public boolean isEmpty()
    {
        FastList<MithraFastList<E>> localLists = lists;
        for (int i = 0; i < localLists.size(); i++)
        {
            if (!localLists.get(i).isEmpty()) return false;
        }
        return true;
    }

    public boolean contains(final Object object)
    {
        FastList<MithraFastList<E>> localLists = lists;
        for (int i = 0; i < localLists.size(); i++)
        {
            if (!localLists.get(i).contains(object)) return true;
        }
        return false;
    }

    public Iterator<E> iterator()
    {
        if (this.lists.isEmpty())
        {
            return Collections.EMPTY_LIST.iterator();
        }
        return new CompositeIterator(this.lists);
    }

    public Object[] toArray()
    {
        final Object[] result = new Object[this.size()];
        int i = 0;
        for (Iterator it = this.iterator(); it.hasNext(); i++)
        {
            result[i] = it.next();
        }
        return result;
    }

    public boolean add(final E object)
    {
        FastList<MithraFastList<E>> localLists = this.lists;
        if (localLists.size() == 0)
        {
            localLists.add(new MithraFastList<E>());
        }
        Collection<E> list = localLists.getLast();
        return list.add(object);
    }

    public boolean remove(final Object object)
    {
        FastList<MithraFastList<E>> localLists = lists;
        for (int i = 0; i < localLists.size(); i++)
        {
            if (localLists.get(i).remove(object)) return true;
        }
        return false;
    }

    public boolean addAll(final Collection<? extends E> collection)
    {
        if (collection.size() > 0)
        {
            if (collection instanceof MithraCompositeList)
            {
                this.lists.addAll(((MithraCompositeList)collection).getLists());
                return true;
            }
            Collection collectionToAdd = collection instanceof MithraFastList ? collection : new MithraFastList<E>(collection);
            this.lists.add((MithraFastList<E>) collectionToAdd);
            return true;
        }
        return false;
    }

    public boolean containsAll(final Collection<?> collection)
    {
        for (Object each : collection)
        {
            if (!this.contains(each)) return false;
        }
        return true;
    }

    public Object[] toArray(final Object[] array)
    {
        int size = this.size();
        Object[] result = null;
        if (array.length >= size)
        {
            result = array;
        }
        else
        {
            result = (Object[]) Array.newInstance(array.getClass().getComponentType(), size);
        }

        int offset = 0;
        FastList<MithraFastList<E>> localLists = this.lists;
        for (int i = 0; i < localLists.size(); ++i)
        {
            for (Iterator it = localLists.get(i).iterator(); it.hasNext();)
            {
                result[offset++] = it.next();
            }
        }
        if (result.length > size)
        {
            result[size] = null;
        }
        return result;
    }

    public boolean addAll(int index, Collection c)
    {
        throw new UnsupportedOperationException();
    }

    public void clear()
    {
        for(int i=0;i<this.lists.size();i++)
        {
            lists.get(i).clear();
        }
    }

    public boolean retainAll(final Collection collection)
    {
        boolean changed = false;
        FastList<MithraFastList<E>> localLists = this.lists;
        for (int i = localLists.size() - 1; i >= 0; i--)
        {
            changed = (localLists.get(i).retainAll(collection) || changed);
        }
        return changed;
    }

    public boolean removeAll(final Collection collection)
    {
        if (collection.isEmpty())
        {
            return false;
        }
        boolean changed = false;
        FastList<MithraFastList<E>> localLists = this.lists;
        for (int i = localLists.size() - 1; i >= 0; i--)
        {
            changed = (localLists.get(i).removeAll(collection) || changed);
        }
        return changed;
    }

    public E get(int index)
    {
        int p = 0;
        FastList<MithraFastList<E>> localLists = this.lists;
        int currentSize = localLists.get(p).size();
        while (index >= currentSize)
        {
            index -= currentSize;
            currentSize = localLists.get(++p).size();
        }
        return localLists.get(p).get(index);
    }

    public E set(int index, E element)
    {
        int p = 0;
        FastList<MithraFastList<E>> localLists = this.lists;
        int currentSize = localLists.get(p).size();
        while (index >= currentSize)
        {
            index -= currentSize;
            currentSize = localLists.get(++p).size();
        }
        return localLists.get(p).set(index, element);
    }

    public void add(int index, E element)
    {
        int max = 0;
        FastList<MithraFastList<E>> localLists = this.lists;
        for (int i = 0; i < localLists.size(); i++)
        {
            List<E> list = localLists.get(i);
            int previousMax = max;
            max += list.size();
            if (index <= max)
            {
                list.add(index - previousMax, element);
                return;
            }
        }
        throw new IndexOutOfBoundsException("No such element " + index + " size: " + this.size());
    }

    public E remove(int index)
    {
        int p = 0;
        FastList<MithraFastList<E>> localLists = this.lists;
        int currentSize = localLists.get(p).size();
        while (index >= currentSize)
        {
            index -= currentSize;
            currentSize = localLists.get(++p).size();
        }
        return localLists.get(p).remove(index);
    }


    public int indexOf(final Object o)
    {
        int offset = 0;
        FastList<MithraFastList<E>> localLists = this.lists;
        for (int i = 0; i < localLists.size(); i++)
        {
            List list = localLists.get(i);
            int index = list.indexOf(o);
            if (index > -1)
            {
                return index + offset;
            }
            offset += list.size();
        }
        return -1;
    }

    public int lastIndexOf(final Object o)
    {
        int offset = this.size();
        FastList<MithraFastList<E>> localLists = this.lists;
        for (int i = localLists.size() - 1; i >= 0; i--)
        {
            List list = localLists.get(i);
            offset -= list.size();
            int index = list.lastIndexOf(o);
            if (index > -1)
            {
                return index + offset;
            }
        }
        return -1;
    }

    /**
     * a list iterator is a problem for a composite list as going back in the order of the list is an issue,
     * as are the other methods like set() and add() (and especially, remove).
     * Convert the internal lists to one list (if not already just one list)
     * and return that list's list iterator.
     * <p/>
     * AFAIK list iterator is only commonly used in sorting.
     *
     * @return a ListIterator for this, with internal state convertedto one list if needed.
     */
    public ListIterator<E> listIterator()
    {
        return this.listIterator(0);
    }

    /**
     * a llst iterator is a problem for a composite list as going back in the order of the list is an issue,
     * as are the other methods like set() and add() (and especially, remove).
     * Convert the internal lists to one list (if not already just one list)
     * and return that list's list iterator.
     * <p/>
     * AFAIK list iterator is only commonly used in sorting.
     *
     * @return a ListIterator for this, with internal state convertedto one list if needed.
     */
    public ListIterator<E> listIterator(int index)
    {
        FastList<MithraFastList<E>> localLists = this.lists;
        if (localLists.size() == 1)
        {
            return localLists.getFirst().listIterator(index);
        }
        if (localLists.isEmpty())
        {
            return Collections.EMPTY_LIST.listIterator(index);
        }
        this.convertMultipleListsToFastList();
        return this.lists.getFirst().listIterator(index);
    }

    /**
     * convert multiple contained lists into one list and replace the contained lists with that list.
     * Synchronize to prevent changes to this list whilst this process is happening
     */
    private void convertMultipleListsToFastList()
    {
        FastList<MithraFastList<E>> localLists = this.lists;
        if (localLists.size() > 1)
        {
            final MithraFastList<E> newList = new MithraFastList<E>(this.size());

            for (int i = 0; i < localLists.size(); i++)
            {
                newList.addAll(localLists.get(i));
            }
            FastList<MithraFastList<E>> newLists = new FastList<MithraFastList<E>>(2);
            newLists.add(newList);
            this.lists = newLists;
        }
    }

    public List<E> subList(int fromIndex, int toIndex)
    {
        convertMultipleListsToFastList();
        return this.lists.get(0).subList(fromIndex, toIndex);
    }

    private class CompositeIterator
            implements Iterator<E>
    {
        private final Iterator<E>[] iterators;
        private Iterator<E> currentIterator;
        private int currentIndex = 0;

        private CompositeIterator(final FastList<MithraFastList<E>> newLists)
        {
            this.iterators = new Iterator[newLists.size()];
            for (int i = 0; i < newLists.size(); ++i)
            {
                this.iterators[i] = newLists.get(i).iterator();
            }
            this.currentIterator = this.iterators[0];
            this.currentIndex = 0;
        }

        public boolean hasNext()
        {
            if (this.currentIterator.hasNext())
            {
                return true;
            }
            else if (this.currentIndex < this.iterators.length - 1)
            {
                this.currentIterator = this.iterators[++this.currentIndex];
                return this.hasNext();
            }
            else
            {
                return false;
            }
        }

        public E next()
        {
            if (this.currentIterator.hasNext())
            {
                return this.currentIterator.next();
            }
            else if (this.currentIndex < this.iterators.length - 1)
            {
                this.currentIterator = this.iterators[++this.currentIndex];
                return this.next();
            }
            else
            {
                throw new NoSuchElementException();
            }
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (!(other instanceof List))
        {
            return false;
        }
        List otherList = (List) other;
        if (this.size() != otherList.size())
        {
            return false;
        }

        Iterator<E> thisIterator = this.iterator();
        Iterator otherIterator = otherList.iterator();
        while (thisIterator.hasNext())
        {
            E thisObject = thisIterator.next();
            Object otherObject = otherIterator.next();
            if (this.notEqual(thisObject, otherObject))
            {
                return false;
            }
        }
        return true;
    }

    private boolean notEqual(final E one, final Object two)
    {
        return !(one == null ? two == null : one.equals(two));
    }

    @Override
    public int hashCode()
    {
        int hashCode = 1;
        Iterator<E> iterator = this.iterator();
        while (iterator.hasNext())
        {
            E item = iterator.next();
            hashCode = 31 * hashCode + (item == null ? 0 : item.hashCode());
        }
        return hashCode;
    }


}
