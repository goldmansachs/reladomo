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

import com.gs.fw.common.mithra.cache.offheap.MasterRetrieveStringResult;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.*;



@SuppressWarnings({"unchecked"})
public class ConcurrentOnHeapStringIndex implements StringIndex
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

    private static final AtomicReferenceFieldUpdater tableUpdater = AtomicReferenceFieldUpdater.newUpdater(ConcurrentOnHeapStringIndex.class, AtomicReferenceArray.class, "table");
    private static final AtomicIntegerFieldUpdater expungerUpdater = AtomicIntegerFieldUpdater.newUpdater(ConcurrentOnHeapStringIndex.class, "expunger");

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    private volatile AtomicReferenceArray table;

    private volatile int expunger; // accessed via expungerUpdater
    private boolean needsCleaning;

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


    public ConcurrentOnHeapStringIndex(int initialCapacity)
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
        try
        {
            needsCleaning = true;
            String.class.getDeclaredField("offset");
        }
        catch (NoSuchFieldException e)
        {
            needsCleaning = false;
        }
        catch(Exception e)
        {
            //ignore
        }
    }

    public ConcurrentOnHeapStringIndex()
    {
        this(DEFAULT_INITIAL_CAPACITY);
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

    @Override
    public void ensureCapacity(int capacity)
    {
        //todo: implement later
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

    @Override
    public int getOffHeapReference(String data)
    {
        throw new RuntimeException("Off String pool not enabled!");
    }

    private String cleanString(String data)
    {
        if (needsCleaning)
        {
            data = new String(data);
        }
        return data;
    }

    @Override
    public String getStringFromOffHeapAddress(int address)
    {
        throw new RuntimeException("Off String pool not enabled!");
    }

    @Override
    public MasterRetrieveStringResult retrieveStrings(int startAddress)
    {
        throw new RuntimeException("Off String pool not enabled!");
    }

    @Override
    public int getIfAbsentPutOffHeap(String data)
    {
        throw new RuntimeException("Off String pool not enabled!");
    }

    public String getIfAbsentPut(String data, boolean hard)
    {
        int hash = data.hashCode();
        AtomicReferenceArray currentArray = getTable();
        String toPut = null;
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
                while (e != null)
                {
                    String candidate = e.get();
                    if (candidate == null)
                    {
                        e = cleanChainAndReturnLeftOver(currentArray, index, (Entry) o, e);
                    }
                    else if (e.getHash() == hash &&
                            candidate.equals(data))
                    {
                        if (hard && e instanceof WeakEntry)
                        {
                            if (toPut == null)
                            {
                                toPut = candidate;
                            }
                            if (!hardenEntry(currentArray, index, (Entry) o, e, toPut))
                            {
                                continue outer;
                            }
                            return toPut;
                        }
                        return candidate;
                    }
                    else
                    {
                        e = e.getNext();
                    }
                }
                if (toPut == null)
                {
                    toPut = cleanString(data);
                }
                Entry newEntry;
                if (hard)
                {
                    newEntry = new HardEntry(toPut, (Entry) o);
                }
                else
                {
                    newEntry = new WeakEntry(toPut, queue, hash, (Entry) o);
                }

                if (updateTable(length, currentArray, index, o, newEntry)) return toPut;
            }
            else
            {
                if (o == null)
                {
                    if (toPut == null)
                    {
                        toPut = cleanString(data);
                    }
                    Object entryOrHardRef;
                    if (hard)
                    {
                        entryOrHardRef = toPut;
                    }
                    else
                    {
                        entryOrHardRef = new WeakEntry(toPut, queue, hash, null);
                    }
                    if (updateTable(length, currentArray, index, o, entryOrHardRef)) return toPut;

                }
                else if (o.equals(data))
                {
                    return (String) o;
                }
                else
                {
                    if (toPut == null)
                    {
                        toPut = cleanString(data);
                    }
                    Entry newEntry;
                    if (hard)
                    {
                        newEntry = new HardEntry(toPut, new HardEntry((String) o, null));
                    }
                    else
                    {
                        newEntry = new WeakEntry(toPut, queue, hash, new HardEntry((String) o, null));
                    }
                    if (updateTable(length, currentArray, index, o, newEntry)) return toPut;
                }
            }
        }
    }

    public void copyTo(StringIndex newPool)
    {
        if (size() == 0) return;
        AtomicReferenceArray currentArray = getTable();
        for(int i=0;i<currentArray.length() - 1;i++)
        {
            Object o = currentArray.get(i);
            if (o == RESIZED || o == RESIZING)
            {
                throw new RuntimeException("cannot copy while resizing!");
            }
            else if (o instanceof  Entry)
            {
                Entry e = (Entry) o;
                while (e != null)
                {
                    String candidate = e.get();
                    if (candidate != null)
                    {
                        newPool.getIfAbsentPut(candidate, e.isHard());
                    }
                    e = e.getNext();
                }
            }
            else if (o != null)
            {
                newPool.getIfAbsentPut((String) o, true);
            }
        }
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

    private boolean updateTable(int length, AtomicReferenceArray currentArray, int index, Object o, Object entryOrHardRef)
    {
        if (currentArray.compareAndSet(index, o, entryOrHardRef))
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
                int removed = 0;
                Entry e = (Entry) o;

                while(e != null && e.get() == null)
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
                Entry replacement = null;
                Entry cur = null;
                while(e != null)
                {
                    Object leftOver = e.get();
                    if (leftOver != null)
                    {
                        if (replacement == null)
                        {
                            replacement = e.cloneWithoutNext(queue);
                            cur = replacement;
                        }
                        else
                        {
                            cur.setNext(e.cloneWithoutNext(queue));
                            cur = cur.getNext();
                        }
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

    private boolean hardenEntry(AtomicReferenceArray table, int index, Entry original, Entry entry, String toPut)
    {
        int removed = 0;
        Entry e = original;

        Entry replacement = null;
        Entry cur = null;
        while(e != null)
        {
            Object leftOver = e.get();
            if (leftOver != null)
            {
                Entry newEntry = e == entry ? new HardEntry(toPut, null) : e.cloneWithoutNext(queue);
                if (replacement == null)
                {
                    replacement = newEntry;
                    cur = replacement;
                }
                else
                {
                    cur.setNext(newEntry);
                    cur = cur.getNext();
                }
            }
            else
            {
                removed++;
            }
            e = e.getNext();
        }
        if (table.compareAndSet(index, original, replacement))
        {
            addToSizeAndGetLocalSize(-removed);
            return true;
        }
        return false;
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
        if (o instanceof Entry)
        {
            Entry e = (Entry) o;
            while (e != null)
            {
                this.unconditionalCopy(dest, e);
                e = e.getNext();
            }
        }
        else
        {
            this.unconditionalCopy(dest, (String) o);
        }
    }

    private void unconditionalCopy(AtomicReferenceArray dest, String s)
    {
        int hash = s.hashCode();
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
            else if (o == null)
            {
                if (currentArray.compareAndSet(index, null, s))
                {
                    return;
                }
            }
            else
            {
                Entry next;
                if (o instanceof Entry)
                {
                    next = (Entry)o;
                }
                else
                {
                    next = new HardEntry((String) o, null);
                }
                if (currentArray.compareAndSet(index, o, new HardEntry(s, next)))
                {
                    return;
                }
            }
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
        Object toCopy = toCopyEntry.get(); // we keep this local variable to ensure it doesn't get collected while transferring.
        if (toCopy == null)
        {
            addToSizeAndGetLocalSize(-1);
            return;
        }
        boolean madeNew = false;
        if (toCopyEntry.getNext() != null)
        {
            toCopyEntry = toCopyEntry.cloneWithoutNext(queue);
            madeNew = true;
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
            else if (o == null && toCopyEntry.isHard())
            {
                if (currentArray.compareAndSet(index, null, toCopy))
                {
                    return;
                }
            }
            else
            {
                if (o != null && !madeNew)
                {
                    toCopyEntry = toCopyEntry.cloneWithoutNext(queue);
                    madeNew = true;
                }
                Entry next = null;
                if (o instanceof Entry)
                {
                    next = (Entry)o;
                }
                else if (o != null)
                {
                    next = new HardEntry((String) o, null);
                }
                toCopyEntry.setNext(next);
                if (currentArray.compareAndSet(index, o, toCopyEntry))
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

    private static interface Entry
    {
        public int getHash();
        public Entry getNext();
        public void setNext(Entry next);
        public Entry cloneWithoutNext(ReferenceQueue queue);
        public String get();

        public boolean isHard();
    }

    private static class WeakEntry extends WeakReference<String> implements Entry
    {
        private final int hash;
        private Entry next;

        public WeakEntry(String key, ReferenceQueue queue,
              int hash, Entry next)
        {
            super(key, queue);
            this.hash = hash;
            this.next = next;
        }

        public boolean isHard()
        {
            return false;
        }

        public int getHash()
        {
            return hash;
        }

        public Entry getNext()
        {
            return next;
        }

        public void setNext(Entry next)
        {
            this.next = next;
        }

        public Entry cloneWithoutNext(ReferenceQueue queue)
        {
            return new WeakEntry(this.get(), queue, this.hash, null);
        }
    }

    private static class HardEntry implements Entry
    {
        private final String value;
        private Entry next;

        public HardEntry(String key, Entry next)
        {
            this.value = key;
            this.next = next;
        }

        public boolean isHard()
        {
            return true;
        }

        public String get()
        {
            return value;
        }

        public int getHash()
        {
            return this.value.hashCode();
        }

        public Entry getNext()
        {
            return next;
        }

        public void setNext(Entry next)
        {
            this.next = next;
        }

        public Entry cloneWithoutNext(ReferenceQueue queue)
        {
            return new HardEntry(this.get(), null);
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