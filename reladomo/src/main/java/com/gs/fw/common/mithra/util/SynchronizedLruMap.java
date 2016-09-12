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


import java.sql.PreparedStatement;

public class SynchronizedLruMap<K, V>
{
    protected static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // must be a power of 2
    protected static final int MAXIMUM_CAPACITY = 1 << 30; // must be a power of 2

    protected static final float DEFAULT_LOAD_FACTOR = 0.75f;

    protected Entry[] table;
    private int size;
    private int threshold;
    private int maxLruSize;
    private int lruSize;
    private Entry lruHead = new Entry(null, null, null);
    private final DestroyAction<V> destroyAction;

    public SynchronizedLruMap(int initialCapacity, int maxLruSize, DestroyAction<V> destroyAction)
    {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal Initial Capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;

        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;
        table = new Entry[capacity];
        threshold = (int) (capacity * DEFAULT_LOAD_FACTOR);
        this.maxLruSize = maxLruSize;
        this.destroyAction = destroyAction;

        lruHead.nextLinked = lruHead.prevLinked = lruHead;
    }

    protected int indexFor(int h, int length)
    {
        return h & (length - 1);
    }

    private Entry[] getTable()
    {
        return table;
    }

    public synchronized int size()
    {
        return size;
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public synchronized boolean forAll(DoUntilProcedure<V> procedure)
    {
        boolean done = false;
        for (int i = 0; i < table.length && !done; i++)
        {
            Entry<K,V> e = table[i];
            while (e != null && !done)
            {
                V candidate = e.value;
                if (candidate != null) done = procedure.execute(candidate);
                e = e.next;
            }
        }
        return done;
    }

    public synchronized V get(K key)
    {
        int hash = key.hashCode();
        int i = indexFor(hash, table.length);
        Entry<K,V> prev = table[i];
        Entry<K,V> e = prev;

        while (true)
        {
            if (e == null)
                return null;
            K candidate = e.getKey();
            if (key.equals(candidate))
            {
                return e.getValue();
            }
            prev = e;
            e = e.next;
        }
    }

    public synchronized V put(K key, V value)
    {
        int hash = key.hashCode();
        Entry[] tab = getTable();
        int i = indexFor(hash, tab.length);

        Entry<K,V> prev = null;
        for (Entry<K,V> e = tab[i]; e != null; e = e.next)
        {
            K candidate = e.getKey();
            if (key.equals(candidate))
            {
                Entry<K,V> newEntry = new Entry(key, value, e.next);
                unlink(e);
                if (e.value != value)
                {
                    this.destroyAction.destroy(e.value);
                }
                link(newEntry);
                if (prev != null)
                {
                    prev.next = newEntry;
                }
                else
                {
                    tab[i] = newEntry;
                }
                return e.getValue();
            }
            prev = e;
        }

        Entry<K,V> newEntry = new Entry(key, value, tab[i]);
        tab[i] = newEntry;
        link(newEntry);
        size++;
        if (size >= threshold)
            resize(table.length * 2);
        return null;
    }

    public synchronized V remove(K key)
    {
        int hash = key.hashCode();
        Entry[] tab = getTable();
        int i = indexFor(hash, tab.length);

        Entry<K,V> prev = null;
        for (Entry<K,V> e = tab[i]; e != null; e = e.next)
        {
            K candidate = e.getKey();
            if (key.equals(candidate))
            {
                unlink(e);
                if (prev == null)
                {
                    table[i] = e.next;
                }
                else
                {
                    prev.next = e.next;
                }
                return e.getValue();
            }
            prev = e;
        }

        return null;
    }

    private void link(Entry e)
    {
        e.nextLinked = lruHead.nextLinked;
        e.prevLinked = lruHead;

        e.prevLinked.nextLinked = e;
        e.nextLinked.prevLinked = e;

        if (lruSize == maxLruSize)
        {
            V removed = remove((K) lruHead.prevLinked.getKey());
            this.destroyAction.destroy(removed);
        }
        lruSize++;
    }

    private void unlink(Entry e)
    {
        lruSize--;
        e.prevLinked.nextLinked = e.nextLinked;
        e.nextLinked.prevLinked = e.prevLinked;

        e.prevLinked = null;
        e.nextLinked = null;
    }

    private void resize(int newCapacity)
    {
        Entry[] oldTable = getTable();
        int oldCapacity = oldTable.length;
        if (oldCapacity == MAXIMUM_CAPACITY)
        {
            threshold = Integer.MAX_VALUE;
            return;
        }

        Entry[] newTable = new Entry[newCapacity];
        transfer(oldTable, newTable);
        table = newTable;

        if (size >= threshold / 2)
        {
            threshold = (int) (newCapacity * DEFAULT_LOAD_FACTOR);
        }
        else
        {
            transfer(newTable, oldTable);
            table = oldTable;
        }
    }

    private void transfer(Entry[] src, Entry[] dest)
    {
        for (int j = 0; j < src.length; ++j)
        {
            Entry<K,V> e = src[j];
            src[j] = null;
            while (e != null)
            {
                Entry next = e.next;
                K key = e.getKey();
                int i = indexFor(key.hashCode(), dest.length);
                e.next = dest[i];
                dest[i] = e;
                e = next;
            }
        }
    }

    public synchronized void clear()
    {
        lruHead.nextLinked = lruHead.prevLinked = lruHead;

        lruSize = 0;

        Entry tab[] = table;
        for (int i = 0; i < tab.length; ++i)
            tab[i] = null;
        size = 0;
    }

    public static interface DestroyAction<V>
    {
        public void destroy(V object);
    }

    protected static class Entry<K, V>
    {
        protected final K key;
        protected final V value;
        protected Entry<K,V> nextLinked;
        protected Entry<K,V> prevLinked;
        protected Entry<K,V> next;

        protected Entry(K key, V value, Entry next)
        {
            this.key = key;
            this.value = value;
            this.next = next;
        }

        public K getKey()
        {
            return this.key;
        }

        public V getValue()
        {
            return value;
        }
    }

}
