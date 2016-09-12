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

package com.gs.fw.common.mithra.connectionmanager;

import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.ListFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;


public class LruListWithThreadAffinity<E>
{

    /**
     * The default initial capacity -- MUST be a power of two.
     */
    protected static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    protected static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The load fast used when none specified in constructor.
     */
    protected static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    protected Entry[] table;

    /**
     * The number of key-value mappings contained in this weak hash map.
     */
    private int size;

    /**
     * The next size value at which to resize (capacity * load factor).
     */
    private int threshold;

    /**
     * The load factor for the hash table.
     */
    protected final float loadFactor;

    private final Random random = new Random(System.currentTimeMillis());

    private Entry lruHead = new Entry(null, 0, null, 0);

    /**
     * Constructs a new, empty <tt>WeakPool</tt> with the given initial
     * capacity and the given load factor.
     *
     * @param initialCapacity The initial capacity of the <tt>WeakHashMap</tt>
     * @param loadFactor      The load factor of the <tt>WeakHashMap</tt>
     * @throws IllegalArgumentException If the initial capacity is negative,
     *                                  or if the load factor is nonpositive.
     */
    public LruListWithThreadAffinity(int initialCapacity, float loadFactor)
    {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal Initial Capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;

        if (loadFactor <= 0 || Float.isNaN(loadFactor))
            throw new IllegalArgumentException("Illegal Load factor: " +
                    loadFactor);

        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;
        table = new Entry[capacity];
        this.loadFactor = loadFactor;
        threshold = (int) (capacity * loadFactor);

        lruHead.nextLinked = lruHead.prevLinked = lruHead;
    }

    /**
     * Constructs a new, empty <tt>WeakPool</tt> with the given initial
     * capacity and the default load factor, which is <tt>0.75</tt>.
     *
     * @throws IllegalArgumentException If the initial capacity is negative.
     */
    public LruListWithThreadAffinity()
    {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    /*
     * Return index for hash code h.
     */
    protected int indexFor(int h, int length)
    {
        return h & (length - 1);
    }

    /*
     * Return the table after first expunging stale entries
     */
    private Entry[] getTable()
    {
        return table;
    }

    /**
     * @return Returns the number of key-value mappings in this map.
     * This result is a snapshot, and may not reflect unprocessed
     * entries that will be removed before next attempted access
     * because they are no longer referenced.
     */
    public synchronized int size()
    {
        return size;
    }

    /**
     * @return Returns <tt>true</tt> if this map contains no key-value mappings.
     * This result is a snapshot, and may not reflect unprocessed
     * entries that will be removed before next attempted access
     * because they are no longer referenced.
     */
    public boolean isEmpty()
    {
        return size() == 0;
    }

    public E remove()
    {
        if (this.size == 0) return null;

        int hash = System.identityHashCode(Thread.currentThread());
        E result = remove(hash);
        if (result == null)
        {
            result = remove(lruHead.prevLinked.hash);
        }
        return result;
    }

    private E remove(int hash)
    {
        Entry[] tab = getTable();
        int i = indexFor(hash, tab.length);
        Entry prev = tab[i];
        Entry<E> e = prev;

        while (e != null)
        {
            Entry<E> next = e.next;
            if (e.hash == hash)
            {
                unlink(e);
                size--;
                if (prev == e)
                    tab[i] = next;
                else
                    prev.next = next;
                return e.key;
            }
            prev = e;
            e = next;
        }
        return null;
    }

    public void add(E key)
    {
        int hash = System.identityHashCode(Thread.currentThread());
        add(key, hash);
    }

    private void add(E key, int hash)
    {
        Entry[] tab = getTable();
        int i = indexFor(hash, tab.length);

        for (Entry e = tab[i]; e != null; e = e.next)
        {
            if (e.hash == hash)
            {
                add(key, random.nextInt());
                return;
            }
        }

        Entry newEntry = new Entry(key, hash, tab[i], System.currentTimeMillis());
        tab[i] = newEntry;
        link(newEntry);
        size++;
        if (size >= threshold)
            resize(table.length * 2);
    }

    private void link(Entry e)
    {
        e.nextLinked = lruHead.nextLinked;
        e.prevLinked = lruHead;

        e.prevLinked.nextLinked = e;
        e.nextLinked.prevLinked = e;
    }

    private void unlink(Entry e)
    {
        e.prevLinked.nextLinked = e.nextLinked;
        e.nextLinked.prevLinked = e.prevLinked;

        e.prevLinked = null;
        e.nextLinked = null;
    }
    /**
     * Rehashes the contents of this map into a new array with a
     * larger capacity.  This method is called automatically when the
     * number of keys in this map reaches its threshold.
     * <p/>
     * If current capacity is MAXIMUM_CAPACITY, this method does not
     * resize the map, but but sets threshold to Integer.MAX_VALUE.
     * This has the effect of preventing future calls.
     *
     * @param newCapacity the new capacity, MUST be a power of two;
     *                    must be greater than current capacity unless current
     *                    capacity is MAXIMUM_CAPACITY (in which case value
     *                    is irrelevant).
     */
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
        threshold = (int) (newCapacity * loadFactor);
    }

    /*
     * Transfer all entries from src to dest tables
     */
    private void transfer(Entry[] src, Entry[] dest)
    {
        for (int j = 0; j < src.length; ++j)
        {
            Entry e = src[j];
            src[j] = null;
            while (e != null)
            {
                Entry next = e.next;
                int i = indexFor(e.hash, dest.length);
                e.next = dest[i];
                dest[i] = e;
                e = next;
            }
        }
    }

    /**
     * Removes all mappings from this map.
     */
    public synchronized void clear()
    {
        lruHead.nextLinked = lruHead.prevLinked = lruHead;

        Entry tab[] = table;
        for (int i = 0; i < tab.length; ++i)
            tab[i] = null;
        size = 0;
    }

    public synchronized boolean forEachUntil(DoUntilProcedure<E> procedure)
    {
        boolean done = false;
        for (int i = 0; i < table.length && !done; i++)
        {
            Entry<E> e = table[i];
            while (e != null && !done)
            {
                E candidate = e.get();
                done = procedure.execute(candidate);
                e = e.next;
            }
        }
        return done;
    }

    public List<E> removeEvictable(long lastAccessTime, int maxToEvict)
    {
        if (lruHead.prevLinked.storeTime < lastAccessTime)
        {
            List result = new ArrayList(maxToEvict);
            while(this.size > 0 && lruHead.prevLinked.storeTime < lastAccessTime && result.size() < maxToEvict)
            {
                result.add(remove(lruHead.prevLinked.hash));
            }
            return result;
        }
        return ListFactory.EMPTY_LIST;
    }

    /**
     * The entries in this hash table extend WeakReference, using its main ref
     * field as the key.
     */
    protected static class Entry<E>
    {
        protected final int hash;
        protected Entry next;
        protected Entry nextLinked;
        protected Entry prevLinked;
        protected E key;
        protected long storeTime;

        public Entry(E key,
              int hash, Entry next, long storeTime)
        {
            this.hash = hash;
            this.next = next;
            this.key = key;
            this.storeTime = storeTime;
        }

        public E get()
        {
            return this.key;
        }

    }


}
