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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.util.DoUntilProcedure;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;


public class LruQueryIndex implements QueryIndex
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

    /**
     * Reference queue for cleared WeakEntries
     */
    private final ReferenceQueue queue = new ReferenceQueue();

    private int maxLruSize;

    private int lruSize;

    private int maxRelationshipSize;

    private int relationshipSize;

    private Entry lruHead = new Entry(this, null, 0, null, false);
    private Entry relationshipHead = new Entry(this, null, 0, null, true);

    private long timeToLive;
    private long relationshipTimeToLive;

    public LruQueryIndex(int initialCapacity, float loadFactor, int maxLruSize, int maxRelationshipSize,
            long timeToLive, long relationshipTimeToLive)
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
        this.maxLruSize = maxLruSize;
        this.maxRelationshipSize = maxRelationshipSize;
        this.timeToLive = timeToLive;
        this.relationshipTimeToLive = relationshipTimeToLive;

        lruHead.nextLinked = lruHead.prevLinked = lruHead;
        relationshipHead.nextLinked = relationshipHead.prevLinked = relationshipHead;
    }

    public LruQueryIndex(int maxLruSize, int maxRelationshipSize, long timeToLive, long relationshipTimeToLive)
    {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, maxLruSize, maxRelationshipSize, timeToLive, relationshipTimeToLive);
    }

    protected int indexFor(int h, int length)
    {
        return h & (length - 1);
    }

    /**
     * Expunge stale entries from the table.
     */
    private synchronized void expungeStaleEntries()
    {
        if (size == 0) return;
        Object r;
        while ((r = queue.poll()) != null)
        {
            Entry e = (Entry) r;
            unlink(e);
            int h = e.hash;
            int i = indexFor(h, table.length);

            Entry prev = table[i];
            Entry p = prev;
            while (p != null)
            {
                Entry next = p.next;
                if (p == e)
                {
                    if (prev == e)
                        table[i] = next;
                    else
                        prev.next = next;
                    e.next = null;  // Help GC
                    size--;
                    break;
                }
                prev = p;
                p = next;
            }
        }
    }

    /**
     * @return Return the table after first expunging stale entries
     */
    private Entry[] getTable()
    {
        expungeStaleEntries();
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
        if (size == 0)
            return 0;
        expungeStaleEntries();
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

    public synchronized boolean forAll(DoUntilProcedure procedure)
    {
        boolean done = false;
        for (int i = 0; i < table.length && !done; i++)
        {
            Entry e = table[i];
            while (e != null && !done)
            {
                Object candidate = e.get();
                if (candidate != null) done = procedure.execute(candidate);
                e = e.next;
            }
        }
        return done;
    }

    public synchronized CachedQuery get(Operation op, boolean forRelationship)
    {
        int hash = op.hashCode();
        int i = indexFor(hash, table.length);
        Entry prev = table[i];
        Entry e = prev;

        long live = timeToLive;
        if (forRelationship) live = relationshipTimeToLive;

        while (true)
        {
            if (e == null)
                return null;
            CachedQuery candidate = e.getCachedQuery();
            if (candidate != null && e.hash == hash &&
                    op.equals(candidate.getOperation()))
            {
                if (candidate.isExpired())
                {
                    unlink(e);
                    size--;
                    if (prev == e)
                        table[i] = e.next;
                    else
                        prev.next = e.next;
                    candidate = null;
                }
                else if (live == 0 || ((TimedEntry)e).isValid(live))
                {
                    relink(e);
                }
                else
                {
                    candidate = null;
                }
                return candidate;
            }
            prev = e;
            e = e.next;
        }
    }

    public synchronized CachedQuery put(CachedQuery key, boolean isForRelationship)
    {
        Operation op = key.getOperation();
        int hash = op.hashCode();
        Entry[] tab = getTable();
        int i = indexFor(hash, tab.length);

        Entry prev = null;
        for (Entry e = tab[i]; e != null; e = e.next)
        {
            CachedQuery candidate = e.getCachedQuery();
            if (candidate != null && e.hash == hash &&
                    op.equals(candidate.getOperation()))
            {
                Entry newEntry = createEntry(key, queue, hash, e.next, isForRelationship || e.isForRelationship);
                unlink(e);
                link(newEntry);
                if (prev != null)
                {
                    prev.next = newEntry;
                }
                else
                {
                    tab[i] = newEntry;
                }
                return candidate;
            }
            prev = e;
        }

        Entry newEntry = createEntry(key, queue, hash, tab[i], isForRelationship);
        tab[i] = newEntry;
        link(newEntry);
        size++;
        if (size >= threshold)
            resize(table.length * 2);
        return null;
    }

    private Entry createEntry(Object key, ReferenceQueue queue, int hash, Entry next, boolean isForRelationship)
    {
        if (timeToLive == 0)
        {
            return new Entry(key, queue, hash, next, isForRelationship);
        }
        else
        {
            return new TimedEntry(key, queue, hash, next, isForRelationship);
        }
    }

    private void link(Entry e)
    {
        if (e.isForRelationship)
        {
            linkForRelationshipHeader(e);
        }
        else
        {
            linkForHeader(lruHead, e, maxLruSize);
        }
    }

    private void linkForHeader(Entry head, Entry e, int maxSize)
    {
        if (maxSize > 0)
        {
            e.hardRef = (CachedQuery) e.get();
            e.nextLinked = head.nextLinked;
            e.prevLinked = head;

            e.prevLinked.nextLinked = e;
            e.nextLinked.prevLinked = e;

            if (lruSize == maxSize)
            {
                unlink(head.prevLinked);
            }
            lruSize++;
        }
    }

    private void linkForRelationshipHeader(Entry e)
    {
        int requestedSize = e.getRequestedSize();
        if (requestedSize < maxRelationshipSize)
        {
            e.hardRef = (CachedQuery) e.get();
            e.nextLinked = relationshipHead.nextLinked;
            e.prevLinked = relationshipHead;

            e.prevLinked.nextLinked = e;
            e.nextLinked.prevLinked = e;

            while (maxRelationshipSize - relationshipSize < requestedSize)
            {
                unlink(relationshipHead.prevLinked);
            }
            relationshipSize += requestedSize;
        }
    }

    private void relink(Entry e)
    {
        unlink(e);
        link(e);
    }

    private void unlink(Entry e)
    {
        if (e.hardRef != null)
        {
            if (e.isForRelationship)
            {
                relationshipSize -= e.getRequestedSize();
            }
            else
            {
                lruSize--;
            }

            e.prevLinked.nextLinked = e.nextLinked;
            e.nextLinked.prevLinked = e.prevLinked;

            e.prevLinked = null;
            e.nextLinked = null;
            e.hardRef = null;

        }
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

        /*
         * If ignoring null elements and processing ref queue caused massive
         * shrinkage, then restore old table.  This should be rare, but avoids
         * unbounded expansion of garbage-filled tables.
         */
        if (size >= threshold / 2)
        {
            threshold = (int) (newCapacity * loadFactor);
        }
        else
        {
            expungeStaleEntries();
            transfer(newTable, oldTable);
            table = oldTable;
        }
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
                Object key = e.get();
                if (key == null)
                {
                    e.next = null;  // Help GC
                    size--;
                }
                else
                {
                    int i = indexFor(e.hash, dest.length);
                    e.next = dest[i];
                    dest[i] = e;
                }
                e = next;
            }
        }
    }

    /**
     * Removes all mappings from this map.
     */
    public synchronized void clear()
    {
        // clear out ref queue. We don't need to expunge entries
        // since table is getting cleared.
        emptySoftQueue();

        lruHead.nextLinked = lruHead.prevLinked = lruHead;
        relationshipHead.nextLinked = relationshipHead.prevLinked = relationshipHead;

        lruSize = 0;
        relationshipSize = 0;

        Entry tab[] = table;
        for (int i = 0; i < tab.length; ++i)
            tab[i] = null;
        size = 0;

        // Allocation of array may have caused GC, which may have caused
        // additional entries to go stale.  Removing these entries from the
        // reference queue will make them eligible for reclamation.
        emptySoftQueue();
    }

    @Override
    public int roughSize()
    {
        return this.size;
    }

    @Override
    public void destroy()
    {
        //nothing to do
    }

    protected void emptySoftQueue()
    {
        while (queue.poll() != null)
            ;
    }

    /*
     * The entries in this hash table extend SoftReference, using its main ref
     * field as the key.
     */
    protected static class Entry extends SoftReference
    {
        protected final int hash;
        protected boolean isForRelationship;
        protected CachedQuery hardRef;
        protected Entry nextLinked;
        protected Entry prevLinked;
        protected Entry next;

        protected Entry(Object key, ReferenceQueue queue,
              int hash, Entry next, boolean isForRelationship)
        {
            super(key, queue);
            this.hash = hash;
            this.next = next;
            this.isForRelationship = isForRelationship;
        }

        public CachedQuery getCachedQuery()
        {
            if (this.hardRef != null) return this.hardRef;
            return (CachedQuery) this.get();
        }

        public int getRequestedSize()
        {
            CachedQuery q = this.getCachedQuery();
            int result = 1;
            if (q != null && q.isOneQueryForMany())
            {
                result = q.getResult().size();
                if (result == 0) result = 1;
            }
            return result;
        }
    }

    protected static class TimedEntry extends Entry
    {
        private long creationTime;

        public TimedEntry(Object key, ReferenceQueue queue, int hash, Entry next, boolean isForRelationship)
        {
            super(key, queue, hash, next, isForRelationship);
            this.creationTime = CacheClock.getTime();
        }

        protected boolean isValid(long live)
        {
            return creationTime + live > CacheClock.getTime();
        }
    }
}
