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

package com.gs.fw.common.mithra.transaction;



public class TransactionLocalMap
{

    /**
     * The entries in this hash map extend WeakReference, using
     * its main ref field as the key (which is always a
     * ThreadLocal object).  Note that null keys (i.e. entry.get()
     * == null) mean that the key is no longer referenced, so the
     * entry can be expunged from table.  Such entries are referred to
     * as "stale entries" in the code that follows.
     */
    private static class Entry
    {
        /**
         * The value associated with this ThreadLocal.
         */
        private TransactionLocal key;
        private Object value;

        Entry(TransactionLocal k, Object v)
        {
            this.key = k;
            value = v;
        }
    }

    /**
     * The initial capacity -- MUST be a power of two.
     */
    private static final int INITIAL_CAPACITY = 16;

    /**
     * The table, resized as necessary.
     * table.length MUST always be a power of two.
     */
    private Entry[] table;

    /**
     * The number of entries in the table.
     */
    private int size = 0;

    /**
     * The next size value at which to resize.
     */
    private int threshold; // Default to 0

    private void setThreshold(int len)
    {
        threshold = len * 2 / 3;
    }

    private static int nextIndex(int i, int len)
    {
        return ((i + 1 < len) ? i + 1 : 0);
    }

    /**
     * Construct a new map initially containing (firstKey, firstValue).
     * TransactionLocalMaps are constructed lazily, so we only create
     * one when we have at least one entry to put in it.
     * @param firstKey key for first entry
     * @param firstValue key for second entry
     */
    public TransactionLocalMap()
    {
        table = new Entry[INITIAL_CAPACITY];
        size = 0;
        setThreshold(INITIAL_CAPACITY);
    }

    /**
     * Get the entry associated with key.  This method
     * itself handles only the fast path: a direct hit of existing
     * key. It otherwise relays to getEntryAfterMiss.  This is
     * designed to maximize performance for direct hits, in part
     * by making this method readily inlinable.
     *
     * @param key the thread local object
     * @return the entry associated with key, or null if no such
     */
    public Object get(TransactionLocal key)
    {
        int i = key.hashCode & (table.length - 1);
        Entry e = table[i];
        if (e != null && e.key == key)
            return e.value;
        else
            return getEntryAfterMiss(key, i, e);
    }

    /**
     * Version of getEntry method for use when key is not found in
     * its direct hash slot.
     *
     * @param key the thread local object
     * @param i   the table index for key's hash code
     * @param e   the entry at table[i]
     * @return the entry associated with key, or null if no such
     */
    private Object getEntryAfterMiss(TransactionLocal key, int i, Entry e)
    {
        Entry[] tab = table;
        int len = tab.length;

        while (e != null)
        {
            if (e.key == key)
                return e.value;
            i = nextIndex(i, len);
            e = tab[i];
        }
        return null;
    }

    /**
     * Set the value associated with key.
     *
     * @param key   the thread local object
     * @param value the value to be set
     */
    public void put(TransactionLocal key, Object value)
    {

        // We don't use a fast path as with get() because it is at
        // least as common to use set() to create new entries as
        // it is to replace existing ones, in which case, a fast
        // path would fail more often than not.

        Entry[] tab = table;
        int len = tab.length;
        int i = key.hashCode & (len - 1);

        for (Entry e = tab[i];
             e != null;
             e = tab[i = nextIndex(i, len)])
        {
            if (e.key == key)
            {
                e.value = value;
                return;
            }
        }

        tab[i] = new Entry(key, value);
        int sz = ++size;
        if (sz >= threshold)
            rehash();
    }

    /**
     * Remove the entry for key.
     * @param key to remove
     */
    public void remove(TransactionLocal key)
    {
        Entry[] tab = table;
        int len = tab.length;
        int i = key.hashCode & (len - 1);
        for (Entry e = tab[i];
             e != null;
             e = tab[i = nextIndex(i, len)])
        {
            if (e.key == key)
            {
                expungeStaleEntry(i);
                return;
            }
        }
    }

    /**
     * Expunge a stale entry by rehashing any possibly colliding entries
     * lying between staleSlot and the next null slot.  This also expunges
     * any other stale entries encountered before the trailing null.  See
     * Knuth, Section 6.4
     *
     * @param staleSlot index of slot known to have null key
     * @return the index of the next null slot after staleSlot
     *         (all between staleSlot and this slot will have been checked
     *         for expunging).
     */
    private int expungeStaleEntry(int staleSlot)
    {
        Entry[] tab = table;
        int len = tab.length;

        // expunge entry at staleSlot
        tab[staleSlot].value = null;
        tab[staleSlot] = null;
        size--;

        // Rehash until we encounter null
        Entry e;
        int i;
        for (i = nextIndex(staleSlot, len);
             (e = tab[i]) != null;
             i = nextIndex(i, len))
        {
            int h = e.key.hashCode & (len - 1);
            if (h != i)
            {
                tab[i] = null;

                // Unlike Knuth 6.4 Algorithm R, we must scan until
                // null because multiple entries could have been stale.
                while (tab[h] != null)
                    h = nextIndex(h, len);
                tab[h] = e;
            }
        }
        return i;
    }

    /**
     * Re-pack and/or re-size the table. First scan the entire
     * table removing stale entries. If this doesn't sufficiently
     * shrink the size of the table, double the table size.
     */
    private void rehash()
    {
        // Use lower threshold for doubling to avoid hysteresis
        if (size >= threshold - threshold / 4)
            resize();
    }

    /**
     * Double the capacity of the table.
     */
    private void resize()
    {
        Entry[] oldTab = table;
        int oldLen = oldTab.length;
        int newLen = oldLen * 2;
        Entry[] newTab = new Entry[newLen];
        int count = 0;

        for (int j = 0; j < oldLen; ++j)
        {
            Entry e = oldTab[j];
            if (e != null)
            {
                int h = e.key.hashCode & (newLen - 1);
                while (newTab[h] != null)
                    h = nextIndex(h, newLen);
                newTab[h] = e;
                count++;
            }
        }

        setThreshold(newLen);
        size = count;
        table = newTab;
    }

}
