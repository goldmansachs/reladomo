
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

package com.gs.fw.common.mithra.cache.offheap;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.block.procedure.primitive.IntObjectProcedure;
import com.gs.collections.impl.list.mutable.FastList;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.*;

public final class ConcurrentOffHeapWeakHolder
{
    private static final long serialVersionUID = 1L;

    private static final Object RESIZE_SENTINEL = new Object();
    private static final int DEFAULT_INITIAL_CAPACITY = 32;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private static final AtomicReferenceFieldUpdater<ConcurrentOffHeapWeakHolder, AtomicReferenceArray> TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(ConcurrentOffHeapWeakHolder.class, AtomicReferenceArray.class, "table");
    private static final AtomicIntegerFieldUpdater<ConcurrentOffHeapWeakHolder> SIZE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ConcurrentOffHeapWeakHolder.class, "size");
    private static final Object RESIZED = new Object();
    private static final Object RESIZING = new Object();
    private static final int PARTITIONED_SIZE_THRESHOLD = 4096; // chosen to keep size below 1% of the total size of the map
    private static final int SIZE_BUCKETS = 7;

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    private volatile AtomicReferenceArray table;

    private AtomicIntegerArray partitionedSize;

    @SuppressWarnings("UnusedDeclaration")
    private volatile int size; // updated via atomic field updater

    public ConcurrentOffHeapWeakHolder()
    {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public ConcurrentOffHeapWeakHolder(int initialCapacity)
    {
        if (initialCapacity < 0)
        {
            throw new IllegalArgumentException("Illegal Initial Capacity: " + initialCapacity);
        }
        if (initialCapacity > MAXIMUM_CAPACITY)
        {
            initialCapacity = MAXIMUM_CAPACITY;
        }

        int threshold = initialCapacity;
        threshold += threshold >> 1; // threshold = length * 0.75

        int capacity = 1;
        while (capacity < threshold)
        {
            capacity <<= 1;
        }
        if (capacity >= PARTITIONED_SIZE_THRESHOLD)
        {
            this.partitionedSize = new AtomicIntegerArray(SIZE_BUCKETS * 16); // we want 7 extra slots and 64 bytes for each slot. int is 4 bytes, so 64 bytes is 16 ints.
        }
        this.table = new AtomicReferenceArray(capacity + 1);
    }

    private static int indexFor(int h, int length)
    {
        return h & length - 2;
    }

    private void incrementSizeAndPossiblyResize(AtomicReferenceArray currentArray, int length, Object prev)
    {
        this.addToSize(1);
        if (prev != null)
        {
            int localSize = this.size();
            int threshold = (length >> 1) + (length >> 2); // threshold = length * 0.75
            if (localSize + 1 > threshold)
            {
                this.resize(currentArray);
            }
        }
    }

    private int hash(WeakOffHeapReference key)
    {
        int h = key.hashCode();
        h ^= h >>> 20 ^ h >>> 12;
        h ^= h >>> 7 ^ h >>> 4;
        return h;
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
    @SuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
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
                    if (this.partitionedSize == null && newSize >= PARTITIONED_SIZE_THRESHOLD)
                    {
                        this.partitionedSize = new AtomicIntegerArray(SIZE_BUCKETS * 16);
                    }
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
            while (!TABLE_UPDATER.compareAndSet(this, oldTable, resizeContainer.nextArray))
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
                Entry e = (Entry) o;
                if (src.compareAndSet(j, o, RESIZING))
                {
                    while (e != null)
                    {
                        this.unconditionalCopy(dest, e);
                        e = e.getNext();
                    }
                    src.set(j, RESIZED);
                    j++;
                }
            }
        }
        resizeContainer.decrementResizerAndNotify();
        resizeContainer.waitForAllResizers();
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
                        Entry e = (Entry) o;
                        if (src.compareAndSet(j, o, RESIZING))
                        {
                            while (e != null)
                            {
                                this.unconditionalCopy(dest, e);
                                e = e.getNext();
                            }
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
        int hash = this.hash(toCopyEntry.getKey());
        AtomicReferenceArray currentArray = dest;
        while (true)
        {
            int length = currentArray.length();
            int index = ConcurrentOffHeapWeakHolder.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = ((ResizeContainer) currentArray.get(length - 1)).nextArray;
            }
            else
            {
                Entry newEntry;
                if (o == null)
                {
                    if (toCopyEntry.getNext() == null)
                    {
                        newEntry = toCopyEntry; // no need to duplicate
                    }
                    else
                    {
                        newEntry = new Entry(toCopyEntry.getKey());
                    }
                }
                else
                {
                    newEntry = new Entry(toCopyEntry.getKey(), (Entry) o);
                }
                if (currentArray.compareAndSet(index, o, newEntry))
                {
                    return;
                }
            }
        }
    }

    private void addToSize(int value)
    {
        if (this.partitionedSize != null)
        {
            if (this.incrementPartitionedSize(value))
            {
                return;
            }
        }
        this.incrementLocalSize(value);
    }

    private boolean incrementPartitionedSize(int value)
    {
        int h = (int) Thread.currentThread().getId();
        h ^= (h >>> 18) ^ (h >>> 12);
        h = (h ^ (h >>> 10)) & SIZE_BUCKETS;
        if (h != 0)
        {
            h = (h - 1) << 4;
            while (true)
            {
                int localSize = this.partitionedSize.get(h);
                if (this.partitionedSize.compareAndSet(h, localSize, localSize + value))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private void incrementLocalSize(int value)
    {
        while (true)
        {
            int localSize = this.size;
            if (SIZE_UPDATER.compareAndSet(this, localSize, localSize + value))
            {
                break;
            }
        }
    }

    public int size()
    {
        int localSize = this.size;
        if (this.partitionedSize != null)
        {
            for (int i = 0; i < SIZE_BUCKETS; i++)
            {
                localSize += this.partitionedSize.get(i << 4);
            }
        }
        return localSize;
    }

    public boolean isEmpty()
    {
        return this.size() == 0;
    }

    private boolean nullSafeEquals(Object v, Object value)
    {
        return v == value || v != null && v.equals(value);
    }

    public void put(WeakOffHeapReference key)
    {
        int hash = this.hash(key);
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = ConcurrentOffHeapWeakHolder.indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null)
        {
            Entry newEntry = new Entry(key, null);
            if (currentArray.compareAndSet(index, null, newEntry))
            {
                this.addToSize(1);
                return;
            }
        }
        this.slowPut(key, hash, currentArray);
    }

    private void slowPut(WeakOffHeapReference key, int hash, AtomicReferenceArray currentArray)
    {
        //noinspection LabeledStatement
        outer:
        while (true)
        {
            int length = currentArray.length();
            int index = ConcurrentOffHeapWeakHolder.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry e = (Entry) o;
                while (e != null)
                {
                    WeakOffHeapReference candidate = e.getKey();
                    if (candidate == key)
                    {
                        Entry newEntry = new Entry(e.getKey(), this.createReplacementChainForRemoval((Entry) o, e));
                        if (!currentArray.compareAndSet(index, o, newEntry))
                        {
                            //noinspection ContinueStatementWithLabel
                            continue outer;
                        }
                        return;
                    }
                    e = e.getNext();
                }
                Entry newEntry = new Entry(key, (Entry) o);
                if (currentArray.compareAndSet(index, o, newEntry))
                {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return;
                }
            }
        }
    }

    public void clear()
    {
        AtomicReferenceArray currentArray = this.table;
        ResizeContainer resizeContainer;
        do
        {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++)
            {
                Object o = currentArray.get(i);
                if (o == RESIZED || o == RESIZING)
                {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
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
                        this.addToSize(-removedEntries);
                    }
                }
            }
            if (resizeContainer != null)
            {
                if (resizeContainer.isNotDone())
                {
                    this.helpWithResize(currentArray);
                    resizeContainer.waitForAllResizers();
                }
                currentArray = resizeContainer.nextArray;
            }
        }
        while (resizeContainer != null);
    }

    public void remove(WeakOffHeapReference key)
    {
        int hash = this.hash(key);
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = ConcurrentOffHeapWeakHolder.indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == RESIZED || o == RESIZING)
        {
            this.slowRemove(key, hash, currentArray);
            return;
        }
        Entry e = (Entry) o;
        while (e != null)
        {
            WeakOffHeapReference candidate = e.getKey();
            if (candidate == key)
            {
                Entry replacement = this.createReplacementChainForRemoval((Entry) o, e);
                if (currentArray.compareAndSet(index, o, replacement))
                {
                    this.addToSize(-1);
                    return;
                }
                this.slowRemove(key, hash, currentArray);
                return;
            }
            e = e.getNext();
        }
    }

    private void slowRemove(WeakOffHeapReference key, int hash, AtomicReferenceArray currentArray)
    {
        //noinspection LabeledStatement
        outer:
        while (true)
        {
            int length = currentArray.length();
            int index = ConcurrentOffHeapWeakHolder.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry e = (Entry) o;
                while (e != null)
                {
                    WeakOffHeapReference candidate = e.getKey();
                    if (candidate == key)
                    {
                        Entry replacement = this.createReplacementChainForRemoval((Entry) o, e);
                        if (currentArray.compareAndSet(index, o, replacement))
                        {
                            this.addToSize(-1);
                            return;
                        }
                        //noinspection ContinueStatementWithLabel
                        continue outer;
                    }
                    e = e.getNext();
                }
                return;
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
                replacement = new Entry(e.getKey(), replacement);
            }
            e = e.getNext();
        }
        return replacement;
    }

    private static final class Entry
    {
        private final WeakOffHeapReference key;
        private final Entry next;

        private Entry(WeakOffHeapReference key)
        {
            this.key = key;
            this.next = null;
        }

        private Entry(WeakOffHeapReference key, Entry next)
        {
            this.key = key;
            this.next = next;
        }

        public WeakOffHeapReference getKey()
        {
            return this.key;
        }

        public Entry getNext()
        {
            return this.next;
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

        public boolean isNotDone()
        {
            return this.resizers.get() > 0;
        }

        public void zeroOutQueuePosition()
        {
            this.queuePosition.set(0);
        }
    }

    public boolean notEmpty()
    {
        return !this.isEmpty();
    }

}
