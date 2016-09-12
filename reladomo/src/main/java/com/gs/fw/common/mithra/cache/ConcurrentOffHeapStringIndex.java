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
import com.gs.fw.common.mithra.util.MithraUnsafe;
import sun.misc.Unsafe;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;



@SuppressWarnings({"unchecked"})
public class ConcurrentOffHeapStringIndex implements StringIndex
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

    private static final AtomicReferenceFieldUpdater TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(ConcurrentOffHeapStringIndex.class, Object[].class, "table");
    private static final AtomicIntegerFieldUpdater expungerUpdater = AtomicIntegerFieldUpdater.newUpdater(ConcurrentOffHeapStringIndex.class, "expunger");

    /**
     * The table, resized as necessary. Length MUST Always be a power of two + 1.
     */
    private volatile Object[] table;

    private int[] partitionedSize;

    private StringToIntMap stringToIntMap = new StringToIntMap();
    
    private volatile int expunger; // accessed via expungerUpdater
    private boolean needsCleaning;

    private int expungeCount;
    private static final int MAX_EXPUNGE_COUNT = 10000;

    private static final Object RESIZED = new Object();
    private static final Object RESIZING = new Object();

    private static final Unsafe UNSAFE;
    private static final long OBJECT_ARRAY_BASE;
    private static final int OBJECT_ARRAY_SHIFT;
    private static final long INT_ARRAY_BASE;
    private static final int INT_ARRAY_SHIFT;
    private static final int SIZE_BUCKETS = 8;

    static
    {
        try
        {
            UNSAFE = MithraUnsafe.getUnsafe();
            Class<?> objectArrayClass = Object[].class;
            OBJECT_ARRAY_BASE = UNSAFE.arrayBaseOffset(objectArrayClass);
            int objectArrayScale = UNSAFE.arrayIndexScale(objectArrayClass);
            if ((objectArrayScale & (objectArrayScale - 1)) != 0)
            {
                throw new AssertionError("data type scale not a power of two");
            }
            OBJECT_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(objectArrayScale);

            Class<?> intArrayClass = int[].class;
            INT_ARRAY_BASE = UNSAFE.arrayBaseOffset(intArrayClass);
            int intArrayScale = UNSAFE.arrayIndexScale(intArrayClass);
            if ((intArrayScale & (intArrayScale - 1)) != 0)
            {
                throw new AssertionError("data type scale not a power of two");
            }
            INT_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(intArrayScale);

        }
        catch (SecurityException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Reference queue for cleared WeakEntries
     */
    private final ReferenceQueue queue = new ReferenceQueue();


    public ConcurrentOffHeapStringIndex(int initialCapacity)
    {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal Initial Capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;

        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;
        table = new Object[capacity+1];
        this.partitionedSize = new int[SIZE_BUCKETS * 16]; // we want 7 extra slots and 64 bytes for each slot. int is 4 bytes, so 64 bytes is 16 ints.
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

    public ConcurrentOffHeapStringIndex()
    {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    private static Object arrayAt(Object[] array, int index)
    {
        return UNSAFE.getObjectVolatile(array, ((long) index << OBJECT_ARRAY_SHIFT) + OBJECT_ARRAY_BASE);
    }

    private static boolean casArrayAt(Object[] array, int index, Object expected, Object newValue)
    {
        return UNSAFE.compareAndSwapObject(array, ((long) index << OBJECT_ARRAY_SHIFT) + OBJECT_ARRAY_BASE, expected, newValue);
    }

    private static void setArrayAt(Object[] array, int index, Object newValue)
    {
        UNSAFE.putObjectVolatile(array, ((long) index << OBJECT_ARRAY_SHIFT) + OBJECT_ARRAY_BASE, newValue);
    }

    private int addToSize(int value)
    {
        int h = (int) Thread.currentThread().getId();
        h ^= (h >>> 18) ^ (h >>> 12);
        h = (h ^ (h >>> 10)) & (SIZE_BUCKETS - 1);
        h = h << 4;
        long address = ((long) h << INT_ARRAY_SHIFT) + INT_ARRAY_BASE;
        while (true)
        {
            int localSize = UNSAFE.getIntVolatile(this.partitionedSize, address);
            if (UNSAFE.compareAndSwapInt(this.partitionedSize, address, localSize, localSize + value))
            {
                return localSize + value;
            }
        }
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
        Object[] table = this.table;
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

    private void removeWeakReferencesAtHash(Object[] table, Entry e, int h)
    {
        while(true)
        {
            int index = indexFor(h, table.length);
            Object o = arrayAt(table, index);
            if (o == RESIZED || o == RESIZED)
            {
                table = ((ResizeContainer) arrayAt(table, table.length - 1)).nextArray;
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
        int localSize = 0;
        for (int i = 0; i < SIZE_BUCKETS; i++)
        {
            localSize += this.partitionedSize[i << 4];
        }
        return localSize;
    }

    @Override
    public void ensureCapacity(int capacity)
    {
        capacity <<= 1;
        Object[] oldTable = this.table;
        int length = oldTable.length;
        while (length < capacity)
        {
            length = (length - 1 << 1) + 1;
        }
        if (length > oldTable.length)
        {
            resize(oldTable, length);
        }
    }

    /*
     * Return the table after first expunging stale entries
     */
    private Object[] getTable()
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
    public String getIfAbsentPut(String data, boolean hard)
    {
        int hash = data.hashCode();
        Object[] currentArray = getTable();
        Entry toPutEntry = null;
outer:
        while(true)
        {
            int length = currentArray.length;
            int index = indexFor(hash, length);
            Object o = arrayAt(currentArray, index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
                continue;
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
                            if (toPutEntry == null)
                            {
                                data =  candidate;
                                toPutEntry = new HardEntry(data, null, stringToIntMap.store(data));
                            }
                            if (!hardenEntry(currentArray, index, (Entry) o, e, toPutEntry))
                            {
                                continue outer;
                            }
                            return candidate;
                        }
                        return candidate;
                    }
                    else
                    {
                        e = e.getNext();
                    }
                }
            }
            if (toPutEntry == null)
            {
                data =  cleanString(data);
                if (hard)
                {
                    toPutEntry = new HardEntry(data, null, stringToIntMap.store(data));
                }
                else
                {
                    toPutEntry = new WeakEntry(data, queue, hash, null);
                }
            }
            toPutEntry.setNext((Entry) o);

            if (updateTable(length, currentArray, index, o, toPutEntry)) return data;
        }
    }

    @Override
    public int getIfAbsentPutOffHeap(String data)
    {
        int hash = data.hashCode();
        Object[] currentArray = getTable();
        Entry toPutEntry = null;
outer:
        while(true)
        {
            int length = currentArray.length;
            int index = indexFor(hash, length);
            Object o = arrayAt(currentArray, index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
                continue;
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
                        if (e instanceof WeakEntry)
                        {
                            if (toPutEntry == null)
                            {
                                data =  candidate;
                                toPutEntry = new HardEntry(data, null, stringToIntMap.store(data));
                            }
                            if (!hardenEntry(currentArray, index, (Entry) o, e, toPutEntry))
                            {
                                continue outer;
                            }
                            return toPutEntry.getOffHeapAddress();
                        }
                        return e.getOffHeapAddress();
                    }
                    else
                    {
                        e = e.getNext();
                    }
                }
            }
            if (toPutEntry == null)
            {
                data =  cleanString(data);
                toPutEntry = new HardEntry(data, null, stringToIntMap.store(data));
            }
            toPutEntry.setNext((Entry) o);

            if (updateTable(length, currentArray, index, o, toPutEntry)) return toPutEntry.getOffHeapAddress();
        }
    }

    private Entry getIfAbsentPutEntry(String data, boolean hard)
    {
        int hash = data.hashCode();
        Object[] currentArray = getTable();
        Entry toPutEntry = null;
outer:
        while(true)
        {
            int length = currentArray.length;
            int index = indexFor(hash, length);
            Object o = arrayAt(currentArray, index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
                continue;
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
                            if (toPutEntry == null)
                            {
                                data =  candidate;
                                toPutEntry = new HardEntry(data, null, stringToIntMap.store(data));
                            }
                            if (!hardenEntry(currentArray, index, (Entry) o, e, toPutEntry))
                            {
                                continue outer;
                            }
                            return toPutEntry;
                        }
                        return e;
                    }
                    else
                    {
                        e = e.getNext();
                    }
                }
            }
            if (toPutEntry == null)
            {
                data =  cleanString(data);
                if (hard)
                {
                    toPutEntry = new HardEntry(data, null, stringToIntMap.store(data));
                }
                else
                {
                    toPutEntry = new WeakEntry(data, queue, hash, null);
                }
            }
            toPutEntry.setNext((Entry) o);

            if (updateTable(length, currentArray, index, o, toPutEntry)) return toPutEntry;
        }
    }

    private String cleanString(String data)
    {
        if (needsCleaning)
        {
            data = new String(data);
        }
        return data;
    }

    private boolean updateTable(int length, Object[] currentArray, int index, Object o, Object entryOrHardRef)
    {
        if (casArrayAt(currentArray, index, o, entryOrHardRef))
        {
            int localSize = addToSize(1);
            if (o != null)
            {
                int threshold = length >> 1;
                threshold += (threshold >> 1); // threshold = length * 0.75
                if (localSize > (threshold >> 3) + 1 && this.size() > threshold)
                {
                    resize(currentArray);
                }
            }
            return true;
        }
        return false;
    }

    private Entry cleanChainAndReturnLeftOver(Object[] table, int index, Entry original, Entry entry)
    {
        if (original == null)
        {
            return null;
        }
        while(true)
        {
            Object o = arrayAt(table, index);
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
                    if (casArrayAt(table, index, original, null))
                    {
                        addToSize(-removed);
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
                if (casArrayAt(table, index, original, replacement))
                {
                    addToSize(-removed);
                    return replacement;
                }
            }
            else
            {
                return entry.getNext();
            }
        }
    }

    private boolean hardenEntry(Object[] table, int index, Entry head, Entry toRemove, Entry toPut)
    {
        int removed = 0;
        Entry e = head;

        Entry replacement = null;
        Entry cur = null;
        while(e != null)
        {
            Object leftOver = e.get();
            if (leftOver != null)
            {
                Entry newEntry = e == toRemove ? toPut : e.cloneWithoutNext(queue);
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
        if (casArrayAt(table, index, head, replacement))
        {
            addToSize(-removed);
            return true;
        }
        return false;
    }

    private Object[] helpWithResizeWhileCurrentIndex(Object[] currentArray, int index)
    {
        Object[] newArray = this.helpWithResize(currentArray);
        int helpCount = 0;
        while (arrayAt(currentArray, index) != RESIZED)
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

    private Object[] helpWithResize(Object[] currentArray)
    {
        ResizeContainer resizeContainer = (ResizeContainer) arrayAt(currentArray, currentArray.length - 1);
        Object[] newTable = resizeContainer.nextArray;
        if (resizeContainer.getQueuePosition() > ResizeContainer.QUEUE_INCREMENT)
        {
            resizeContainer.incrementResizer();
            this.reverseTransfer(currentArray, resizeContainer);
            resizeContainer.decrementResizerAndNotify();
        }
        return newTable;
    }

    private void resize(Object[] oldTable)
    {
        this.resize(oldTable, (oldTable.length - 1 << 1) + 1);
    }

    // newSize must be a power of 2 + 1
    private void resize(Object[] oldTable, int newSize)
    {
        int oldCapacity = oldTable.length;
        int end = oldCapacity - 1;
        Object last = arrayAt(oldTable, end);
        if (this.size() < end && last == RESIZE_SENTINEL)
        {
            return;
        }
        if (oldCapacity >= MAXIMUM_CAPACITY)
        {
            throw new RuntimeException("max capacity of map exceeded");
        }
        ResizeContainer resizeContainer = null;
        boolean ownResize = false;
        if (last == null || last == RESIZE_SENTINEL)
        {
            synchronized (oldTable) // allocating a new array is too expensive to make this an atomic operation
            {
                if (arrayAt(oldTable, end) == null)
                {
                    setArrayAt(oldTable, end, RESIZE_SENTINEL);
                    resizeContainer = new ResizeContainer(new Object[newSize], oldTable.length - 1);
                    setArrayAt(oldTable, end, resizeContainer);
                    ownResize = true;
                }
            }
        }
        if (ownResize)
        {
            this.transfer(oldTable, resizeContainer);

            Object[] src = this.table;
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

    private void transfer(Object[] src, ResizeContainer resizeContainer)
    {
        Object[] dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length - 1; )
        {
            Object o = arrayAt(src, j);
            if (o == null)
            {
                if (casArrayAt(src, j, null, RESIZED))
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
                if (casArrayAt(src, j, o, RESIZING))
                {
                    while (e != null)
                    {
                        this.unconditionalCopy(dest, e);
                        e = e.getNext();
                    }
                    setArrayAt(src, j, RESIZED);
                    j++;
                }
            }
        }
        resizeContainer.decrementResizerAndNotify();
        resizeContainer.waitForAllResizers();
    }


    private void reverseTransfer(Object[] src, ResizeContainer resizeContainer)
    {
        Object[] dest = resizeContainer.nextArray;
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
                    Object o = arrayAt(src, j);
                    if (o == null)
                    {
                        if (casArrayAt(src, j, null, RESIZED))
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
                        if (casArrayAt(src, j, o, RESIZING))
                        {
                            while (e != null)
                            {
                                this.unconditionalCopy(dest, e);
                                e = e.getNext();
                            }
                            setArrayAt(src, j, RESIZED);
                            j--;
                        }
                    }
                }
            }
        }
    }

    private void unconditionalCopy(Object[] dest, Entry toCopyEntry)
    {
        Object toCopy = toCopyEntry.get(); // we keep this local variable to ensure it doesn't get collected while transferring.
        if (toCopy == null)
        {
            addToSize(-1);
            return;
        }
        boolean madeNew = false;
        if (toCopyEntry.getNext() != null)
        {
            toCopyEntry = toCopyEntry.cloneWithoutNext(queue);
            madeNew = true;
        }
        int hash = toCopyEntry.getHash();
        Object[] currentArray = dest;
        while (true)
        {
            int length = currentArray.length;
            int index = indexFor(hash, length);
            Object o = arrayAt(currentArray, index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = ((ResizeContainer) arrayAt(currentArray, length - 1)).nextArray;
            }
            else
            {
                if (o != null && !madeNew)
                {
                    toCopyEntry = toCopyEntry.cloneWithoutNext(queue);
                    madeNew = true;
                }
                toCopyEntry.setNext((Entry) o);
                if (casArrayAt(currentArray, index, o, toCopyEntry))
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

    @Override
    public int getOffHeapReference(String data)
    {
        int hash = data.hashCode();
        Object[] currentArray = getTable();
        while(true)
        {
            int length = currentArray.length;
            int index = indexFor(hash, length);
            Object o = arrayAt(currentArray, index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
                continue;
            }
            else if (o instanceof Entry)
            {
                Entry e = (Entry) o;
                while (e != null)
                {
                    String candidate = e.get();
                    if (candidate != null && e.getHash() == hash &&
                            candidate.equals(data))
                    {
                        return e.getOffHeapAddress();
                    }
                    e = e.getNext();
                }
            }
            return StringIndex.UNKNOWN_STRING;
        }
    }

    private static interface Entry
    {
        public int getHash();
        public Entry getNext();
        public void setNext(Entry next);
        public Entry cloneWithoutNext(ReferenceQueue queue);
        public String get();
        public int getOffHeapAddress();

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

        @Override
        public int getOffHeapAddress()
        {
            return StringIndex.UNKNOWN_STRING;
        }
    }

    private static class HardEntry implements Entry
    {
        private final String value;
        private Entry next;
        private final int offHeapAddress;

        public HardEntry(String key, Entry next, int offHeapAddress)
        {
            this.value = key;
            this.next = next;
            this.offHeapAddress = offHeapAddress;
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

        public int getOffHeapAddress()
        {
            return offHeapAddress;
        }

        public void setNext(Entry next)
        {
            this.next = next;
        }

        public Entry cloneWithoutNext(ReferenceQueue queue)
        {
            return new HardEntry(this.get(), null, this.offHeapAddress);
        }
    }

    private static final class ResizeContainer
    {
        private static final int QUEUE_INCREMENT = Math.min(1 << 10, Integer.highestOneBit(Runtime.getRuntime().availableProcessors()) << 4);
        private final AtomicInteger resizers = new AtomicInteger(1);
        private final Object[] nextArray;
        private final AtomicInteger queuePosition;

        private ResizeContainer(Object[] nextArray, int oldSize)
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
                            //ginore
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

    @Override
    public String getStringFromOffHeapAddress(int address)
    {
        return stringToIntMap.list[address];
    }

    @Override
    public MasterRetrieveStringResult retrieveStrings(int startAddress)
    {
        int maxAddress = this.getMaxOffHeapStringAddress();
        int size = maxAddress - startAddress + 1;
        MasterRetrieveStringResult result = new MasterRetrieveStringResult(size);
        for(int i=0;i<size;i++)
        {
            result.addString(i, startAddress + i, this.stringToIntMap.list[startAddress + i]);
        }
        return result;
    }

    private int getMaxOffHeapStringAddress()
    {
        return stringToIntMap.getMaxStringAddress();
    }

    private static class StringToIntMap
    {
        private AtomicInteger end = new AtomicInteger(1);
        private String[] list = new String[32000];

        public int store(String aString)
        {
            int pos = end.incrementAndGet();
            if (pos == list.length)
            {
                //we own the resize
                String[] newArray = new String[pos << 1];
                System.arraycopy(list, 0, newArray, 0, list.length);
                synchronized (this)
                {
                    list = newArray;
                    this.notifyAll();
                }
            }
            else if (pos > list.length)
            {
                synchronized (this)
                {
                    while(pos > list.length)
                    {
                        try
                        {
                            this.wait();
                        }
                        catch (InterruptedException e)
                        {
                            //ignore
                        }
                    }
                }
            }
            list[pos] = aString;
            return pos;
        }


        public int getMaxStringAddress()
        {
            String[] localList = this.list;
            int max = end.get();
            if (max >= localList.length)
            {
                max = localList.length - 1;
            }
            while(max >= 0 && localList[max] == null)
            {
                max--;
            }
            return max;
        }
    }

}