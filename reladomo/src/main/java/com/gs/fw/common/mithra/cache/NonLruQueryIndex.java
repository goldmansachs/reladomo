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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.*;


public class NonLruQueryIndex implements Evictable, QueryIndex
{
    private static final Object RESIZE_SENTINEL = new Object();
    /**
     * The default initial capacity -- MUST be a power of two + 1.
     */
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private static final AtomicReferenceFieldUpdater tableUpdater = AtomicReferenceFieldUpdater.newUpdater(NonLruQueryIndex.class, AtomicReferenceArray.class, "table");
    private static final AtomicIntegerFieldUpdater expungerUpdater = AtomicIntegerFieldUpdater.newUpdater(NonLruQueryIndex.class, "expunger");

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    private volatile AtomicReferenceArray table;

    private volatile int expunger;

    private int expungeCount;
    private static final int MAX_EXPUNGE_COUNT = 10000;

    private static final Object RESIZED = new Object();
    private static final Object RESIZING = new Object();
    private static final int PARTITIONED_SIZE_THRESHOLD = 4096; // chosen to keep size below 1% of the total size of the map
    private static final int SIZE_BUCKETS = 8;

    private AtomicIntegerArray partitionedSize = new AtomicIntegerArray(SIZE_BUCKETS*16); // each cache line is 64 bytes. we want one integer in each cache line


    /**
     * Reference queue for cleared WeakEntries
     */
    private final ReferenceQueue queue = new ReferenceQueue();

    private int weakThreshold = Cache.WEAK_THRESHOLD;
    private final ReferenceListener listener;

    public NonLruQueryIndex(int initialCapacity)
    {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal Initial Capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;

        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;
        table = new AtomicReferenceArray(capacity+1);
        this.listener = new ReferenceListener()
        {
            @Override
            public void evictCollectedReferences()
            {
                NonLruQueryIndex.this.evictCollectedReferences();
            }
        };
        MithraReferenceThread.getInstance().addListener(listener);
    }

    public NonLruQueryIndex()
    {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public CachedQuery get(Operation op, boolean forRelationship)
    {
        int hash = op.hashCode();
        AtomicReferenceArray currentArray = getTable();
        while(true)
        {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else if (o instanceof Entry)
            {
                Entry e = (Entry) o;
                while (e != null)
                {
                    CachedQuery candidate = e.getUnexpiredCachedQuery();
                    if (candidate == null)
                    {
                        e = cleanChainAndReturnLeftOver(currentArray, index, (Entry) o, e);
                    }
                    else if (e.getHash() == hash &&
                            candidate.getOperation().equals(op))
                    {
                        return candidate;
                    }
                    else
                    {
                        e = e.getNext();
                    }
                }
                return null;
            }
            else // o is null
            {
                return null;
            }
        }
    }

    public CachedQuery put(CachedQuery key, boolean isForRelationship)
    {
        int hash = key.getOperation().hashCode();
        boolean soft =  (key.getResult().size() < weakThreshold);

        AtomicReferenceArray currentArray = getTable();
        outer:
        while(true)
        {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else if (o instanceof Entry)
            {
                Entry e = (Entry) o;
                Entry next = e;
                boolean replaced = false;
                while (e != null)
                {
                    CachedQuery candidate = e.get();
                    if (candidate == null || candidate.isExpired())
                    {
                        e = cleanChainAndReturnLeftOver(currentArray, index, (Entry) o, e);
                    }
                    else if (e.getHash() == hash &&
                            candidate.getOperation().equals(key.getOperation()))
                    {
                        next = createReplacementChainForRemoval((Entry) o, e);
                        replaced = true;
                        break;
                    }
                    else
                    {
                        e = e.getNext();
                    }
                }
                Entry newEntry;
                if (soft)
                {
                    newEntry = new SoftEntry(key, next, hash, queue);
                }
                else
                {
                    newEntry = new WeakEntry(key, queue, hash, next);
                }

                if (updateTable(length, currentArray, index, o, newEntry, replaced)) return key;
            }
            else // o is null
            {
                Object entry;
                if (soft)
                {
                    entry = new SoftEntry(key, null, hash, queue);
                }
                else
                {
                    entry = new WeakEntry(key, queue, hash, null);
                }
                if (updateTable(length, currentArray, index, o, entry, false)) return key;
            }
        }
    }

    private Entry createReplacementChainForRemoval(Entry original, Entry toRemove)
    {
        if (original == toRemove)
        {
            return original.getNext();
        }
        Entry replacement = null;
        Entry e = original;
        while (e != null)
        {
            if (e != toRemove)
            {
                if (replacement == null)
                {
                    replacement = e.cloneWithoutNext(queue);
                }
                else
                {
                    replacement = e.cloneWithNext(queue, replacement);
                }
            }
            e = e.getNext();
        }
        return replacement;
    }

    public void clear()
    {
        AtomicReferenceArray currentArray = this.table;
        boolean followResize;
        do
        {
            followResize = false;
            for (int i = 0; i < currentArray.length() - 1; i++)
            {
                Object o = currentArray.get(i);
                if (o == RESIZED || o == RESIZING)
                {
                    followResize = true;
                    break;
                }
                else if (o != null)
                {
                    Entry e = (Entry) o;
                    if (currentArray.compareAndSet(i, o, null))
                    {
                        int removedEntries = 0;
                        while (e != null)
                        {
                            removedEntries++;
                            e = e.getNext();
                        }
                        addToSizeAndGetLocalSize(-removedEntries);
                    }
                    else
                    {
                        i--; // retry this one
                    }
                }
            }
            if (followResize)
            {
                helpWithResize(currentArray);
                ResizeContainer resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                resizeContainer.waitForAllResizers();
                currentArray = resizeContainer.nextArray;
            }
        }
        while (followResize);
    }

    @Override
    public int roughSize()
    {
        return this.size();
    }

    @Override
    public void destroy()
    {
        zRemoveListener();
    }

    public void zRemoveListener()
    {
        MithraReferenceThread.getInstance().removeListener(listener);
    }


    /*
     * Return index for hash code h.
     */
    private static int indexFor(int h, int length)
    {
        return h & (length - 2);
    }

    /*
     * Expunge stale entries from the table.
     */
    private boolean expungeStaleEntries()
    {
        Object r;
        boolean result = false;
        AtomicReferenceArray table = this.table;
        while ((r = queue.poll()) != null)
        {
            result = true;
            Entry e = (Entry) r;
            int h = e.getHash();
            removeWeakReferencesAtHash(table, e, h);
        }
        expungerUpdater.set(this, 0);
        return result;
    }

    private void removeWeakReferencesAtHash(AtomicReferenceArray table, Entry e, int h)
    {
        while(true)
        {
            int index = indexFor(h, table.length());
            Object o = table.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                table = ((ResizeContainer) table.get(table.length() - 1)).nextArray;
                continue;
            }
            if (o instanceof Entry)
            {
                cleanChainAndReturnLeftOver(table, index, (Entry) o, e);
            }
            break;
        }
    }

    public int size()
    {
        int size = 0;
        for(int i=0;i<SIZE_BUCKETS;i++)
        {
            size += partitionedSize.get(i << 4);
        }
        return size;
    }

    /*
     * Return the table after first expunging stale entries
     */
    private AtomicReferenceArray getTable()
    {
        expungeCount++;
        if (expungeCount >= MAX_EXPUNGE_COUNT)
        {
            expungeCount = 0;
            if (expungerUpdater.compareAndSet(this, 0, 1))
            {
                MithraConcurrentEvictorThread.getInstance().queueEviction(this);
            }
        }
        return table;
    }

    private int addToSizeAndGetLocalSize(int value)
    {
        int h = (int) Thread.currentThread().getId();
        h ^= (h >>> 18) ^ (h >>> 12);
        h = (h ^ (h >>> 10)) & (SIZE_BUCKETS - 1);
        h = h << 4;
        while (true)
        {
            int localSize = this.partitionedSize.get(h);
            if (this.partitionedSize.compareAndSet(h, localSize, localSize + value))
            {
                return localSize + value;
            }
        }
    }

    private boolean updateTable(int length, AtomicReferenceArray currentArray, int index, Object o, Object entryOrHardRef, boolean replaced)
    {
        if (currentArray.compareAndSet(index, o, entryOrHardRef))
        {
            if (!replaced)
            {
                int localSize = addToSizeAndGetLocalSize(1);
                if (o != null)
                {
                    int threshold = length >> 1;
                    threshold += (threshold >> 1); // threshold = length * 0.75
                    if (localSize > (threshold >> 3) + 1 && size() > threshold)
                    {
                        resize(currentArray);
                    }
                }
            }
            return true;
        }
        return false;
    }

    private Entry cleanChainAndReturnLeftOver(AtomicReferenceArray table, int index, Entry original, Entry entry)
    {
        if (original == null)
        {
            return null;
        }
        while(true)
        {
            Object o = table.get(index);
            if (o == original)
            {
                Entry e = (Entry) o;
                Entry cleanTail = getCleanTail(e);

                int removed = 0;
                e = (Entry) o;

                if (cleanTail == null)
                {
                    while(e != null && e.getUnexpiredCachedQuery() == null)
                    {
                        removed++;
                        e = e.getNext();
                    }
                    if (e == null)
                    {
                        if (table.compareAndSet(index, original, null))
                        {
                            addToSizeAndGetLocalSize(-removed);
                            return null;
                        }
                    }
                }
                e = (Entry) o;
                removed = 0;
                Entry replacement = cleanTail;
                while(e != cleanTail)
                {
                    CachedQuery validCachedQuery = e.getUnexpiredCachedQuery();
                    if (validCachedQuery != null)
                    {
                        replacement = e.cloneWithNext(queue, replacement);
                    }
                    else
                    {
                        removed++;
                    }
                    e = e.getNext();
                }
                if (removed == 0) return original.getNext();
                if (table.compareAndSet(index, original, replacement))
                {
                    addToSizeAndGetLocalSize(-removed);
                    return replacement;
                }
            }
            else
            {
                return entry.getNext();
            }
        }
    }

    private Entry getCleanTail(Entry e)
    {
        Entry cleanTail = e;
        while(e != null)
        {
            CachedQuery cachedQuery = e.getUnexpiredCachedQuery();
            if (cachedQuery == null)
            {
                cleanTail = e.getNext();
            }
            e = e.getNext();
        }
        return cleanTail;
    }

    private AtomicReferenceArray helpWithResizeWhileCurrentIndex(AtomicReferenceArray currentArray, int index)
    {
        AtomicReferenceArray newArray = this.helpWithResize(currentArray);
        int helpCount = 0;
        while (currentArray.get(index) != RESIZED)
        {
            helpCount++;
            newArray = this.helpWithResize(currentArray);
            if ((helpCount & 7) == 0)
            {
                Thread.yield();
            }
        }
        return newArray;
    }

    private AtomicReferenceArray helpWithResize(AtomicReferenceArray currentArray)
    {
        ResizeContainer resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
        AtomicReferenceArray newTable = resizeContainer.nextArray;
        if (resizeContainer.getQueuePosition() > ResizeContainer.QUEUE_INCREMENT)
        {
            resizeContainer.incrementResizer();
            this.reverseTransfer(currentArray, resizeContainer);
            resizeContainer.decrementResizerAndNotify();
        }
        return newTable;
    }

    private void resize(AtomicReferenceArray oldTable)
    {
        this.resize(oldTable, (oldTable.length() - 1 << 1) + 1);
    }

    // newSize must be a power of 2 + 1
    private void resize(AtomicReferenceArray oldTable, int newSize)
    {
        int oldCapacity = oldTable.length();
        int end = oldCapacity - 1;
        Object last = oldTable.get(end);
        if (this.size() < end && last == RESIZE_SENTINEL)
        {
            return;
        }
        if (oldCapacity >= MAXIMUM_CAPACITY)
        {
            throw new RuntimeException("index is too large!");
        }
        ResizeContainer resizeContainer = null;
        boolean ownResize = false;
        if (last == null || last == RESIZE_SENTINEL)
        {
            synchronized (oldTable) // allocating a new array is too expensive to make this an atomic operation
            {
                if (oldTable.get(end) == null)
                {
                    oldTable.set(end, RESIZE_SENTINEL);
                    resizeContainer = new ResizeContainer(new AtomicReferenceArray(newSize), oldTable.length() - 1);
                    oldTable.set(end, resizeContainer);
                    ownResize = true;
                }
            }
        }
        if (ownResize)
        {
            this.transfer(oldTable, resizeContainer);
            AtomicReferenceArray src = this.table;
            while (!tableUpdater.compareAndSet(this, oldTable, resizeContainer.nextArray))
            {
                // we're in a double resize situation; we'll have to go help until it's our turn to set the table
                if (src != oldTable)
                {
                    this.helpWithResize(src);
                }
            }
        }
        else
        {
            this.helpWithResize(oldTable);
        }
    }

    /*
     * Transfer all entries from src to dest tables
     */
    private void transfer(AtomicReferenceArray src, ResizeContainer resizeContainer)
    {
        AtomicReferenceArray dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length() - 1; )
        {
            Object o = src.get(j);
            if (o == null)
            {
                if (src.compareAndSet(j, null, RESIZED))
                {
                    j++;
                }
            }
            else if (o == RESIZED || o == RESIZING)
            {
                j = (j & ~(ResizeContainer.QUEUE_INCREMENT - 1)) + ResizeContainer.QUEUE_INCREMENT;
                if (resizeContainer.resizers.get() == 1)
                {
                    break;
                }
            }
            else
            {
                if (src.compareAndSet(j, o, RESIZING))
                {
                    moveBucket(o, dest);
                    src.set(j, RESIZED);
                    j++;
                }
            }
        }
        resizeContainer.decrementResizerAndNotify();
        resizeContainer.waitForAllResizers();
    }

    private void moveBucket(Object o, AtomicReferenceArray dest)
    {
        Entry e = (Entry) o;
        while (e != null)
        {
            this.unconditionalCopy(dest, e);
            e = e.getNext();
        }
    }

    private void reverseTransfer(AtomicReferenceArray src, ResizeContainer resizeContainer)
    {
        AtomicReferenceArray dest = resizeContainer.nextArray;
        while (resizeContainer.getQueuePosition() > 0)
        {
            int start = resizeContainer.subtractAndGetQueuePosition();
            int end = start + ResizeContainer.QUEUE_INCREMENT;
            if (end > 0)
            {
                if (start < 0)
                {
                    start = 0;
                }
                for (int j = end - 1; j >= start; )
                {
                    Object o = src.get(j);
                    if (o == null)
                    {
                        if (src.compareAndSet(j, null, RESIZED))
                        {
                            j--;
                        }
                    }
                    else if (o == RESIZED || o == RESIZING)
                    {
                        resizeContainer.zeroOutQueuePosition();
                        return;
                    }
                    else
                    {
                        if (src.compareAndSet(j, o, RESIZING))
                        {
                            moveBucket(o, dest);
                            src.set(j, RESIZED);
                            j--;
                        }
                    }
                }
            }
        }
    }

    private void unconditionalCopy(AtomicReferenceArray dest, Entry toCopyEntry)
    {
        CachedQuery toCopy = toCopyEntry.getUnexpiredCachedQuery(); // we keep this local variable to ensure it doesn't get collected while transferring.
        if (toCopy == null)
        {
            addToSizeAndGetLocalSize(-1);
            return;
        }
        int hash = toCopyEntry.getHash();
        AtomicReferenceArray currentArray = dest;
        while(true)
        {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = ((ResizeContainer) currentArray.get(length - 1)).nextArray;
            }
            else
            {
                Entry next = (Entry)o;
                if (currentArray.compareAndSet(index, o, toCopyEntry.cloneWithNext(queue, next)))
                {
                    return;
                }
            }
        }

    }

    public boolean evictCollectedReferences()
    {
        return this.expungeStaleEntries();
    }

    public int getEntryCount()
    {
        AtomicReferenceArray table = this.table;
        int result = 0;
        for(int i=0;i<table.length();i++)
        {
            Object o = table.get(i);
            Entry e = (Entry) o;
            while(e != null)
            {
                result ++;
                e = e.getNext();
            }
        }
        return result;
    }

    private static interface Entry
    {
        public int getHash();
        public Entry getNext();
        public Entry cloneWithoutNext(ReferenceQueue queue);
        public CachedQuery get();

        public CachedQuery getUnexpiredCachedQuery();

        public Entry cloneWithNext(ReferenceQueue queue, Entry next);
    }

    private static class WeakEntry extends WeakReference<CachedQuery> implements Entry
    {
        private final int hash;
        private final Entry next;

        public WeakEntry(CachedQuery key, ReferenceQueue queue,
              int hash, Entry next)
        {
            super(key, queue);
            this.hash = hash;
            this.next = next;
        }

        public int getHash()
        {
            return hash;
        }

        public Entry getNext()
        {
            return next;
        }

        @Override
        public CachedQuery getUnexpiredCachedQuery()
        {
            CachedQuery cachedQuery = this.get();
            return cachedQuery != null && !cachedQuery.isExpired() ? cachedQuery : null;
        }

        public Entry cloneWithoutNext(ReferenceQueue queue)
        {
            return cloneWithNext(queue, null);
        }

        @Override
        public Entry cloneWithNext(ReferenceQueue queue, Entry next)
        {
            if (this.next == next)
            {
                return this;
            }
            return new WeakEntry(this.get(), queue, this.hash, next);
        }
    }

    private static class SoftEntry extends SoftReference<CachedQuery> implements Entry
    {
        private final int hash;
        private final Entry next;

        public SoftEntry(CachedQuery key, Entry next, int hash, ReferenceQueue queue)
        {
            super(key, queue);
            this.hash = hash;
            this.next = next;
        }

        public int getHash()
        {
            return hash;
        }

        public Entry getNext()
        {
            return next;
        }

        @Override
        public CachedQuery getUnexpiredCachedQuery()
        {
            CachedQuery cachedQuery = this.get();
            return cachedQuery != null && !cachedQuery.isExpired() ? cachedQuery : null;
        }

        public Entry cloneWithoutNext(ReferenceQueue queue)
        {
            return cloneWithNext(queue, null);
        }

        public Entry cloneWithNext(ReferenceQueue queue, Entry next)
        {
            if (this.next == next)
            {
                return this;
            }
            return new SoftEntry(this.get(), next, hash, queue);
        }
    }

    private static final class ResizeContainer
    {
        private static final int QUEUE_INCREMENT = Math.min(1 << 10, Integer.highestOneBit(Runtime.getRuntime().availableProcessors()) << 4);
        private final AtomicInteger resizers = new AtomicInteger(1);
        private final AtomicReferenceArray nextArray;
        private final AtomicInteger queuePosition;

        private ResizeContainer(AtomicReferenceArray nextArray, int oldSize)
        {
            this.nextArray = nextArray;
            this.queuePosition = new AtomicInteger(oldSize);
        }

        public void incrementResizer()
        {
            this.resizers.incrementAndGet();
        }

        public void decrementResizerAndNotify()
        {
            int remaining = this.resizers.decrementAndGet();
            if (remaining == 0)
            {
                synchronized (this)
                {
                    this.notifyAll();
                }
            }
        }

        public int getQueuePosition()
        {
            return this.queuePosition.get();
        }

        public int subtractAndGetQueuePosition()
        {
            return this.queuePosition.addAndGet(-QUEUE_INCREMENT);
        }

        public void waitForAllResizers()
        {
            if (this.resizers.get() > 0)
            {
                for (int i = 0; i < 16; i++)
                {
                    if (this.resizers.get() == 0)
                    {
                        break;
                    }
                }
                for (int i = 0; i < 16; i++)
                {
                    if (this.resizers.get() == 0)
                    {
                        break;
                    }
                    Thread.yield();
                }
            }
            if (this.resizers.get() > 0)
            {
                synchronized (this)
                {
                    while (this.resizers.get() > 0)
                    {
                        try
                        {
                            this.wait();
                        }
                        catch (InterruptedException e)
                        {
                            // ignore
                        }
                    }
                }
            }
        }

        public void zeroOutQueuePosition()
        {
            this.queuePosition.set(0);
        }
    }
}