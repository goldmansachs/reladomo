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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedObject;
import com.gs.fw.common.mithra.MithraDatedObjectFactory;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MithraUnsafe;
import sun.misc.Unsafe;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;



@SuppressWarnings({"unchecked"})
public class ConcurrentDatedObjectIndex implements Evictable
{
    private CommonExtractorBasedHashingStrategy hashStrategy;
    private AsOfAttribute[] asOfAttributes;
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

    private static final AtomicReferenceFieldUpdater TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(ConcurrentDatedObjectIndex.class, Object[].class, "table");
    private static final AtomicIntegerFieldUpdater expungerUpdater = AtomicIntegerFieldUpdater.newUpdater(ConcurrentDatedObjectIndex.class, "expunger");
    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    private volatile Object[] table;
    private volatile int expunger;

    private boolean forceWeak = false;

    private final int[] partitionedSize = new int[SIZE_BUCKETS * 16]; // we want 64 bytes for each slot. int is 4 bytes, so 64 bytes is 16 ints.

    private MithraDatedObjectFactory factory;

    private int expungeCount;
    private int weakThreshold = Cache.WEAK_THRESHOLD;
    private final long currentDataOffset;
    private static final int MAX_EXPUNGE_COUNT = 10000;

    private static final Object RESIZED = new Object();
    private static final Object RESIZING = new Object();

    /*
     * Reference queue for cleared WeakEntries
     */
    private final ReferenceQueue queue = new ReferenceQueue();
    private static final Object RESIZE_SENTINEL = new Object();


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

    public ConcurrentDatedObjectIndex(CommonExtractorBasedHashingStrategy hashStrategy, AsOfAttribute[] asOfAttributes,
            MithraDatedObjectFactory factory, int initialCapacity)
    {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal Initial Capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;

        this.asOfAttributes = asOfAttributes;
        this.hashStrategy = hashStrategy;
        this.factory = factory;
        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;
        table = new Object[capacity+1];
        this.currentDataOffset = MithraUnsafe.findCurrentDataOffset(asOfAttributes[0]);
    }

    public ConcurrentDatedObjectIndex(Extractor[] extractors, AsOfAttribute[] asOfAttributes, MithraDatedObjectFactory factory)
    {
        this(ExtractorBasedHashStrategy.create(extractors), asOfAttributes, factory, DEFAULT_INITIAL_CAPACITY);
    }

    private static int indexFor(int h, int length)
    {
        return h & (length - 2);
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

    private int addToSizeReturnLocalSize(int value)
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
            if (o == RESIZED || o == RESIZING)
            {
                table = ((ResizeContainer) arrayAt(table, table.length - 1)).nextArray;
                continue;
            }
            if (o instanceof Entry)
            {
                this.cleanChainAndReturnLeftOver(table, index, (Entry) o, e);
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

    /*
     * Return the table after first expunging stale entries
     */
    private Object[] getTable()
    {
        expungeCount++;
        if (expungeCount >= MAX_EXPUNGE_COUNT)
        {
            forceWeak = this.size() > weakThreshold;
            expungeCount = 0;
            if (expungerUpdater.compareAndSet(this, 0, 1))
            {
                MithraConcurrentEvictorThread.getInstance().queueEviction(this);
            }
        }
        return table;
    }

    protected int computeHashCodeFromData(int nonDatedPkHashCode, Timestamp[] asOfDates)
    {
        int hashCode = nonDatedPkHashCode;
        for (int i = 0; i < asOfDates.length; i++)
        {
            hashCode = HashUtil.combineHashes(hashCode, asOfDates[i].hashCode());
        }
        return hashCode;

    }

    protected int computeHashCodeFromObject(Object mithraObject)
    {
        Object data = this.getData(mithraObject);
        int hashCode = hashStrategy.computeHashCode(data);
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            hashCode = HashUtil.combineHashes(hashCode, asOfAttributes[i].valueHashCode(mithraObject));
        }
        return hashCode;

    }

    protected int computeHashCodeFromObject(Object mithraObject, int nonPkHashCode)
    {
        int hashCode = nonPkHashCode;
        for (int i = 0; i < asOfAttributes.length; i++)
        {
            hashCode = HashUtil.combineHashes(hashCode, asOfAttributes[i].valueHashCode(mithraObject));
        }
        return hashCode;

    }

    protected boolean candidateMatches(Object target, Object candidate)
    {
        Object targetData = this.getData(target);
        if (targetData == null) return false;
        Object candidateData = this.getData(candidate);
        if (candidateData == null) return false;

        boolean result = this.hashStrategy.equals(targetData, candidateData);
        for (int i = 0; result && i < asOfAttributes.length; i++)
        {
            result = asOfAttributes[i].valueEquals(target, candidate);
        }
        return result;
    }

    private Object getData(Object candidate)
    {
        MithraDataObject result = (MithraDataObject) UNSAFE.getObject(candidate, currentDataOffset);
        if (result == null)
        {
            MithraDatedObject obj = (MithraDatedObject) candidate;
            result = obj.zGetCurrentOrTransactionalData();
        }
        return result;
    }

    public Object getFromDataOrPutIfAbsent(Object data, Timestamp[] asOfDates, int nonDatedPkHashCode, boolean weak)
    {
        weak = weak || forceWeak;
        int hash = this.computeHashCodeFromData(nonDatedPkHashCode, asOfDates);
        Object[] currentArray = getTable();
        Object toPut = null;
        while(true)
        {
            int length = currentArray.length;
            int index = indexFor(hash, length);
            Object o = arrayAt(currentArray, index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry e = (Entry) o;
                while (e != null)
                {
                    Object candidate = e.get();
                    if (candidate == null)
                    {
                        e = cleanChainAndReturnLeftOver(currentArray, index, (Entry) o, e);
                    }
                    else if (e.getHash() == hash &&
                            ((MithraDatedObject) candidate).zDataMatches(data, asOfDates))
                    {
                        return candidate;
                    }
                    else
                    {
                        e = e.getNext();
                    }
                }
                if (toPut == null)
                {
                    toPut = factory.createObject((MithraDataObject) data, asOfDates);
                }
                Entry newEntry;
                if (weak)
                {
                    newEntry = new WeakEntry(toPut, queue, hash, (Entry) o);
                }
                else
                {
                    newEntry = new SoftEntry(toPut, queue, hash, (Entry) o);
                }
                if (casArrayAt(currentArray, index, o, newEntry))
                {
                    incrementSizeAndOptionallyResize(currentArray, length, o);
                    return toPut;
                }
            }
        }
    }

    private void incrementSizeAndOptionallyResize(Object[] currentArray, int length, Object o)
    {
        int localSize = addToSizeReturnLocalSize(1);
        if (o != null)
        {
            int threshold = length >> 1;
            threshold += (threshold >> 1); // threshold = length * 0.75
            if (localSize > (threshold >> 3) + 1 && this.size() > threshold)
            {
                resize(currentArray);
            }
        }
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
                        addToSizeReturnLocalSize(-removed);
                        return null;
                    }
                    return entry.getNext();
                }
                Entry removeCheck = e;
                while(removeCheck != null)
                {
                    if (removeCheck.get() == null)
                    {
                        break;
                    }
                    removeCheck = removeCheck.getNext();
                }
                if (removeCheck == null)
                {
                    // there is nothing to clean!
                    if (e != original)
                    {
                        if (casArrayAt(table, index, original, e))
                        {
                            addToSizeReturnLocalSize(-removed);
                            return e;
                        }
                        return entry.getNext();
                    }
                    return e;
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
                    addToSizeReturnLocalSize(-removed);
                    return replacement;
                }
            }
            else
            {
                return entry.getNext();
            }
        }
    }

    public Object put(Object key, int nonDatedPkHashCode, boolean weak)
    {
        weak = weak || forceWeak;
        int hash = this.computeHashCodeFromObject(key, nonDatedPkHashCode);
        Object[] currentArray = getTable();
outer:        
        while(true)
        {
            int length = currentArray.length;
            int index = indexFor(hash, length);
            Object o = arrayAt(currentArray, index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry e = (Entry) o;
                while (e != null)
                {
                    Object candidate = e.get();
                    if (candidate == null)
                    {
                        e = cleanChainAndReturnLeftOver(currentArray, index, (Entry) o, e);
                    }
                    else if (e.getHash() == hash &&
                            this.candidateMatches(key, candidate))
                    {
                        Entry replacement = createReplacementChain((Entry) o, e, key);
                        if (casArrayAt(currentArray, index, o, replacement))
                        {
                            return candidate;
                        }
                        else
                        {
                            continue outer;
                        }
                    }
                    else
                    {
                        e = e.getNext();
                    }
                }
                Entry newEntry;
                if (weak)
                {
                    newEntry = new WeakEntry(key, queue, hash, (Entry) o);
                }
                else
                {
                    newEntry = new SoftEntry(key, queue, hash, (Entry) o);
                }
                if (casArrayAt(currentArray, index, o, newEntry))
                {
                    incrementSizeAndOptionallyResize(currentArray, length, o);
                    return null;
                }
            }
        }
    }

    private Entry createReplacementChain(Entry original, Entry toReplace, Object key)
    {
        Entry replacement = null;
        Entry cur = null;
        Entry e = original;
        while(e != null)
        {
            Object candidate = e.get();
            if (e == toReplace)
            {
                candidate = key;
            }
            if (replacement == null)
            {
                replacement = e.cloneWithoutNext(candidate, queue);
                cur = replacement;
            }
            else
            {
                cur.setNext(e.cloneWithoutNext(candidate, queue));
                cur = cur.getNext();
            }
            e = e.getNext();
        }
        return replacement;
    }

    public Object remove(Object key)
    {
        int hash = this.computeHashCodeFromObject(key);
        Object[] currentArray = getTable();
outer:        
        while(true)
        {
            int length = currentArray.length;
            int index = indexFor(hash, length);
            Object o = arrayAt(currentArray, index);
            if (o == RESIZED || o == RESIZING)
            {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
            }
            else
            {
                Entry e = (Entry) o;
                while (e != null)
                {
                    Object candidate = e.get();
                    if (candidate == null)
                    {
                        e = cleanChainAndReturnLeftOver(currentArray, index, (Entry) o, e);
                    }
                    else if (e.getHash() == hash &&
                            this.candidateMatches(key, candidate))
                    {
                        Entry replacement = createReplacementChainForRemoval((Entry) o, e);
                        if (casArrayAt(currentArray, index, o, replacement))
                        {
                            addToSizeReturnLocalSize(-1);
                            return candidate;
                        }
                        else
                        {
                            continue outer;
                        }
                    }
                    else
                    {
                        e = e.getNext();
                    }
                }
                return null;
            }
        }
    }

    private Entry createReplacementChainForRemoval(Entry original, Entry toRemove)
    {
        if (original == toRemove && original.getNext() == null)
        {
            return null;
        }
        Entry replacement = null;
        Entry cur = null;
        Entry e = original;
        while(e != null)
        {
            if (e != toRemove)
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
            e = e.getNext();
        }
        return replacement;
    }

    public boolean evictCollectedReferences()
    {
        return this.expungeStaleEntries();
    }

    public int getEntryCount()
    {
        Object[] table = this.table;
        int result = 0;
        for(int i=0;i<table.length;i++)
        {
            Entry e = (Entry) arrayAt(table, i);
            while(e != null)
            {
                result ++;
                e = e.getNext();
            }
        }
        return result;
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
            addToSizeReturnLocalSize(-1);
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

    /**
     * The entries in this hash table extend WeakReference, using its main ref
     * field as the key.
     */
    private static interface Entry
    {
        public int getHash();
        public Object get();
        public Entry getNext();
        public void setNext(Entry next);
        public Entry cloneWithoutNext(ReferenceQueue queue);
        public Entry cloneWithoutNext(Object key, ReferenceQueue queue);
    }

    private static class SoftEntry extends SoftReference implements Entry
    {
        private final int hash;
        private Entry next;

        public SoftEntry(Object key, ReferenceQueue queue,
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
            return new SoftEntry(this.get(), queue, this.hash, null);
        }

        public Entry cloneWithoutNext(Object key, ReferenceQueue queue)
        {
            return new SoftEntry(key, queue, this.hash, null);
        }
    }

    private static class WeakEntry extends WeakReference implements Entry
    {
        private final int hash;
        private Entry next;

        public WeakEntry(Object key, ReferenceQueue queue,
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

        public Entry cloneWithoutNext(Object key, ReferenceQueue queue)
        {
            return new WeakEntry(key, queue, this.hash, null);
        }
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
}